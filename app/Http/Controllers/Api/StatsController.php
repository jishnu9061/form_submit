<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Http\Resources\HourlyStatResource;
use App\Models\FormHourlyCount;
use Illuminate\Http\Resources\Json\AnonymousResourceCollection;

/**
 * Returns live hourly submission counts for a given form_id.
 * Data is written by the Flink job via JDBC upsert — this endpoint
 * just reads from MySQL without any aggregation overhead.
 */
class StatsController extends Controller
{
    /**
     * GET /api/forms/{form_id}/stats
     */
    public function hourly(string $formId): AnonymousResourceCollection
    {
        $stats = FormHourlyCount::where('form_id', $formId)
            ->orderBy('hour_start', 'asc')
            ->get();

        return HourlyStatResource::collection($stats);
    }
}
