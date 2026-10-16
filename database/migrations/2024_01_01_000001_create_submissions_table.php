<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * Creates the submissions table.
 *
 * Design decisions:
 *  - BIGINT UNSIGNED auto-increment primary key — future-proof for 80k+ rows.
 *  - idempotency_key UNIQUE constraint is the race-safe duplicate guard.
 *    MySQL enforces uniqueness at the storage level; INSERT IGNORE or
 *    insertOrIgnore() returns without error on a duplicate key, letting us
 *    fetch and return the existing row instead of throwing.
 *  - data JSON column — MySQL 8 native JSON; allows schema-free form payloads
 *    while still being queryable with JSON_EXTRACT / ->>.
 *  - Composite index (form_id, id) supports the pagination query
 *    "WHERE form_id = ? ORDER BY id DESC" efficiently on large tables.
 */
return new class extends Migration
{
    public function up(): void
    {
        Schema::create('submissions', function (Blueprint $table) {
            $table->id();                                        // BIGINT UNSIGNED AUTO_INCREMENT PK
            $table->string('form_id', 100);
            $table->string('idempotency_key', 100)->unique();   // UNIQUE — duplicate guard
            $table->string('email');
            $table->json('data');                                // MySQL native JSON column
            $table->timestamps();

            // Composite index for paginated listing per form_id ordered newest-first
            $table->index(['form_id', 'id'], 'submissions_form_id_id_idx');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('submissions');
    }
};
