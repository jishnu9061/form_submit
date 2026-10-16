<?php

namespace App\Services;

use App\Models\Outbox;
use App\Models\Submission;
use Illuminate\Support\Facades\DB;

/**
 * SubmissionService — all business logic lives here, not in the controller.
 *
 * Single Responsibility: the controller handles HTTP concerns (request/response);
 * this service handles the business rules (idempotency, outbox, etc.).
 */
class SubmissionService
{
    /**
     * Store a new submission (or detect a duplicate) atomically.
     *
     * Race-safety design:
     *   We use insertOrIgnore() which maps to MySQL's INSERT IGNORE statement.
     *   If two concurrent requests arrive with the same idempotency_key, MySQL's
     *   UNIQUE constraint ensures only one INSERT succeeds; the other gets a
     *   silent duplicate-key error (MySQL errno 1062) and returns 0 affected rows.
     *   We then SELECT the existing row — this avoids the classic check-then-insert
     *   TOCTOU race condition where two threads both pass the check and both insert.
     *
     * Outbox pattern:
     *   The outbox row is written inside the SAME transaction as the submission row.
     *   If the process crashes before the outbox worker publishes to Kafka, the row
     *   persists and the worker will pick it up on the next poll. This guarantees
     *   at-least-once delivery without distributed transactions (2PC).
     *
     * @return array{submission: Submission, is_duplicate: bool}
     */
    public function createOrFindDuplicate(
        string $formId,
        string $idempotencyKey,
        string $email,
        array  $data,
    ): array {
        $inserted = false;

        DB::transaction(function () use ($formId, $idempotencyKey, $email, $data, &$inserted) {
            // INSERT IGNORE — silently no-ops on duplicate idempotency_key
            $affected = DB::table('submissions')->insertOrIgnore([
                'form_id'         => $formId,
                'idempotency_key' => $idempotencyKey,
                'email'           => $email,
                'data'            => json_encode($data),
                'created_at'      => now(),
                'updated_at'      => now(),
            ]);

            $inserted = $affected > 0;

            if ($inserted) {
                // Fetch the row we just created to get the auto-generated id
                $submission = Submission::where('idempotency_key', $idempotencyKey)->first();

                // Write outbox row in the SAME transaction — atomic with the insert.
                // The worker will publish this to Kafka asynchronously.
                Outbox::create([
                    'topic'   => 'submissions',
                    'payload' => [
                        'id'         => $submission->id,
                        'form_id'    => $submission->form_id,
                        'email'      => $submission->email,
                        'created_at' => $submission->created_at->toIso8601String(),
                    ],
                ]);
            }
        });

        $submission = Submission::where('idempotency_key', $idempotencyKey)->first();

        return [
            'submission'   => $submission,
            'is_duplicate' => ! $inserted,
        ];
    }
}