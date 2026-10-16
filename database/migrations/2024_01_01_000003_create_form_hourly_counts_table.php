<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * Flink sink table — stores hourly submission counts per form.
 *
 * PRIMARY KEY (form_id, hour_start) enables MySQL's idempotent upsert:
 *   INSERT INTO form_hourly_counts ... ON DUPLICATE KEY UPDATE count = VALUES(count)
 *
 * When Flink replays from a checkpoint, it re-emits the same (form_id, hour,
 * count) and the upsert simply overwrites the existing row — no double counting.
 */
return new class extends Migration
{
    public function up(): void
    {
        Schema::create('form_hourly_counts', function (Blueprint $table) {
            $table->string('form_id', 100);
            $table->dateTime('hour_start');
            $table->unsignedBigInteger('count')->default(0);
            $table->timestamp('updated_at')->useCurrent()->useCurrentOnUpdate();

            // Composite PK — the upsert key for idempotent Flink writes
            $table->primary(['form_id', 'hour_start']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('form_hourly_counts');
    }
};
