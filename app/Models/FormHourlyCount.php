<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

/**
 * @property string $form_id
 * @property \Carbon\Carbon $hour_start
 * @property int $count
 * @property \Carbon\Carbon $updated_at
 */
class FormHourlyCount extends Model
{
    protected $table = 'form_hourly_counts';

    // No auto-increment PK — composite (form_id, hour_start)
    public $incrementing = false;
    public $timestamps   = false;

    protected $primaryKey = null; // Eloquent won't use find() but it keeps code clean

    protected $fillable = [
        'form_id',
        'hour_start',
        'count',
    ];

    protected $casts = [
        'hour_start' => 'datetime',
        'updated_at' => 'datetime',
    ];
}
