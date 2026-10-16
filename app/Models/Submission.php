<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

/**
 * @property int    $id
 * @property string $form_id
 * @property string $idempotency_key
 * @property string $email
 * @property array  $data
 * @property \Carbon\Carbon $created_at
 * @property \Carbon\Carbon $updated_at
 */
class Submission extends Model
{
    protected $fillable = [
        'form_id',
        'idempotency_key',
        'email',
        'data',
    ];

    /**
     * Cast the JSON column to a PHP array automatically.
     * MySQL stores it as validated JSON; Laravel hydrates it as array.
     */
    protected $casts = [
        'data' => 'array',
    ];
}
