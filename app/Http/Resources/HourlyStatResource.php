<?php

namespace App\Http\Resources;

use Illuminate\Http\Request;
use Illuminate\Http\Resources\Json\JsonResource;

class HourlyStatResource extends JsonResource
{
    public function toArray(Request $request): array
    {
        return [
            'form_id'    => $this->form_id,
            'hour_start' => \Carbon\Carbon::parse($this->hour_start)->toIso8601String(),
            'count'      => (int) $this->count,
        ];
    }
}
