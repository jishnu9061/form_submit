<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

/**
 * @property int    $id
 * @property string $topic
 * @property array  $payload
 * @property \Carbon\Carbon|null $published_at
 * @property \Carbon\Carbon $created_at
 */
class Outbox extends Model
{
    public $timestamps = false;

    protected $fillable = [
        'topic',
        'payload',
    ];

    protected $casts = [
        'payload'      => 'array',
        'published_at' => 'datetime',
        'created_at'   => 'datetime',
    ];
}
