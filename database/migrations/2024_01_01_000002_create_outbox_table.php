<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * Creates the outbox table for reliable Kafka publishing.
 *
 * The Outbox Pattern:
 *   Instead of publishing directly to Kafka inside the HTTP request (which
 *   would lose the event if Kafka is down), we write an outbox row in the
 *   SAME transaction as the submission insert. A separate artisan worker polls
 *   this table, publishes each row to Kafka, then marks it published.
 *
 *   Guarantee: if the app crashes between DB commit and Kafka publish, the
 *   outbox row survives the crash, and the worker will retry. This gives
 *   at-least-once delivery to Kafka. The Flink idempotent upsert sink and the
 *   API-level idempotency_key prevent double-counting downstream.
 *
 * - payload JSON: the full event body (id, form_id, email, created_at).
 * - published_at NULL → pending; non-NULL → delivered.
 * - Index on published_at covers the "WHERE published_at IS NULL" poll query.
 */
return new class extends Migration
{
    public function up(): void
    {
        Schema::create('outbox', function (Blueprint $table) {
            $table->id();
            $table->string('topic', 100)->default('submissions');
            $table->json('payload');
            $table->timestamp('published_at')->nullable();
            $table->timestamp('created_at')->useCurrent();

            $table->index(['published_at', 'id'], 'outbox_pending_idx');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('outbox');
    }
};
