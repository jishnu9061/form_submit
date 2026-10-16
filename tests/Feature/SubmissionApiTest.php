<?php

use App\Models\Submission;
use Illuminate\Foundation\Testing\RefreshDatabase;

/*
|--------------------------------------------------------------------------
| Submission API Feature Tests
|--------------------------------------------------------------------------
| These tests run against a real MySQL `app_testing` database (not SQLite)
| so UNIQUE constraint enforcement and INSERT IGNORE semantics are identical
| to production. RefreshDatabase wraps each test in a transaction that is
| rolled back, keeping tests fast and isolated.
*/

uses(RefreshDatabase::class);

// ---------------------------------------------------------------------------
// Validation — expects HTTP 400 (overridden in StoreSubmissionRequest)
// ---------------------------------------------------------------------------

it('returns 400 when email is invalid', function () {
    $this->postJson('/api/submissions', [
        'form_id'         => 'contact-form',
        'idempotency_key' => 'test-key-1',
        'email'           => 'not-an-email',
        'data'            => ['name' => 'Test'],
    ])->assertStatus(400)
      ->assertJsonStructure(['message', 'errors' => ['email']]);
});

it('returns 400 when form_id is missing', function () {
    $this->postJson('/api/submissions', [
        'idempotency_key' => 'test-key-2',
        'email'           => 'user@example.com',
        'data'            => ['name' => 'Test'],
    ])->assertStatus(400)
      ->assertJsonStructure(['errors' => ['form_id']]);
});

it('returns 400 when idempotency_key is missing', function () {
    $this->postJson('/api/submissions', [
        'form_id' => 'contact-form',
        'email'   => 'user@example.com',
        'data'    => ['name' => 'Test'],
    ])->assertStatus(400)
      ->assertJsonStructure(['errors' => ['idempotency_key']]);
});

it('returns 400 when data is missing', function () {
    $this->postJson('/api/submissions', [
        'form_id'         => 'contact-form',
        'idempotency_key' => 'test-key-4',
        'email'           => 'user@example.com',
    ])->assertStatus(400)
      ->assertJsonStructure(['errors' => ['data']]);
});

// ---------------------------------------------------------------------------
// Successful create — expects HTTP 201
// ---------------------------------------------------------------------------

it('creates a submission and returns 201', function () {
    $payload = [
        'form_id'         => 'contact-form',
        'idempotency_key' => 'unique-key-abc',
        'email'           => 'user@example.com',
        'data'            => ['name' => 'John', 'message' => 'Hello'],
    ];

    $response = $this->postJson('/api/submissions', $payload);

    $response->assertStatus(201)
             ->assertJsonStructure(['data' => ['id', 'form_id', 'email', 'data', 'created_at']])
             ->assertJsonPath('data.form_id', 'contact-form')
             ->assertJsonPath('data.email', 'user@example.com');

    $this->assertDatabaseHas('submissions', [
        'form_id'         => 'contact-form',
        'idempotency_key' => 'unique-key-abc',
    ]);
});

// ---------------------------------------------------------------------------
// Duplicate — expects HTTP 200 with duplicate:true and the SAME record id
// ---------------------------------------------------------------------------

it('returns 200 with duplicate:true and same id on duplicate key', function () {
    $payload = [
        'form_id'         => 'contact-form',
        'idempotency_key' => 'duplicate-key-xyz',
        'email'           => 'user@example.com',
        'data'            => ['name' => 'John'],
    ];

    // First request — creates the record
    $first = $this->postJson('/api/submissions', $payload);
    $first->assertStatus(201);
    $firstId = $first->json('data.id');

    // Second request with the same idempotency_key
    $second = $this->postJson('/api/submissions', $payload);
    $second->assertStatus(200)
           ->assertJsonPath('duplicate', true)
           ->assertJsonPath('data.id', $firstId); // same record returned

    // Exactly one row in the DB — not two
    $this->assertDatabaseCount('submissions', 1);
});

// ---------------------------------------------------------------------------
// Pagination — GET /api/forms/{form_id}/submissions
// ---------------------------------------------------------------------------

it('paginates submissions for a form with 10 per page', function () {
    // Create 15 submissions for the same form
    for ($i = 1; $i <= 15; $i++) {
        Submission::create([
            'form_id'         => 'pagination-form',
            'idempotency_key' => "pag-key-{$i}",
            'email'           => "user{$i}@example.com",
            'data'            => ['index' => $i],
        ]);
    }

    $response = $this->getJson('/api/forms/pagination-form/submissions');

    $response->assertStatus(200)
             ->assertJsonStructure(['data', 'links'])
             ->assertJsonCount(10, 'data'); // max 10 per page
});

it('returns empty data for a form with no submissions', function () {
    $this->getJson('/api/forms/unknown-form/submissions')
         ->assertStatus(200)
         ->assertJsonCount(0, 'data');
});
