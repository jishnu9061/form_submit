<?php

namespace App\Console\Commands;

use App\Models\Outbox;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Junges\Kafka\Facades\Kafka;
use Junges\Kafka\Message\Message as KafkaMessage;

/**
 * Outbox publisher worker.
 *
 * This command polls the `outbox` table for un-published rows and publishes
 * each payload to the Kafka topic recorded on the row. After a successful
 * Kafka publish it marks the row as published by setting published_at.
 *
 * Why a separate process instead of direct Kafka publish in the HTTP handler?
 *   → The Outbox Pattern decouples the HTTP request from the Kafka transport:
 *     1. If Kafka is temporarily unavailable, the submission is still saved and
 *        the outbox row survives. This worker will retry on the next poll.
 *     2. The DB insert + outbox write are atomic (same transaction), so there is
 *        no window where a submission is saved but no event will ever be sent.
 *     3. At-least-once delivery: if this process crashes after Kafka publish but
 *        before updating published_at, it will re-publish on restart. The Flink
 *        JDBC sink is idempotent (ON DUPLICATE KEY UPDATE), so the duplicate
 *        event is harmless downstream.
 *
 * Options:
 *   --batch  Number of rows to fetch per poll (default: 100)
 *   --sleep  Seconds to sleep between polls when queue is empty (default: 2)
 *   --once   Process one batch then exit (useful in tests / cron)
 */
class OutboxPublishCommand extends Command
{
    protected $signature = 'outbox:publish
                            {--batch=100 : Rows per poll batch}
                            {--sleep=2   : Sleep seconds between empty polls}
                            {--once      : Run one batch only}';

    protected $description = 'Publish pending outbox rows to Kafka (Outbox Pattern worker)';

    public function handle(): int
    {
        $batchSize = (int) $this->option('batch');
        $sleepSecs = (int) $this->option('sleep');
        $once      = (bool) $this->option('once');

        $this->info('Outbox publisher started. Batch=' . $batchSize);

        do {
            $rows = Outbox::whereNull('published_at')
                ->orderBy('id')
                ->limit($batchSize)
                ->get();

            if ($rows->isEmpty()) {
                if ($once) {
                    break;
                }
                sleep($sleepSecs);
                continue;
            }

            foreach ($rows as $row) {
                try {
                    $this->publishToKafka($row->topic, $row->payload);

                    $row->published_at = now();
                    $row->save();
                } catch (\Throwable $e) {
                    // Log and continue — the row stays un-published and will be retried.
                    Log::error('Outbox publish failed', [
                        'outbox_id' => $row->id,
                        'error'     => $e->getMessage(),
                    ]);
                }
            }

            $this->line('Published ' . $rows->count() . ' outbox rows.');
        } while (! $once);

        return self::SUCCESS;
    }

    private function publishToKafka(string $topic, array $payload): void
    {
        $message = new KafkaMessage(
            topicName: $topic,
            body: $payload,
            key: (string) $payload['id'],
        );

        Kafka::publish(config('kafka.brokers'))
            ->onTopic($topic)
            ->withMessage($message)
            ->send();
    }
}
