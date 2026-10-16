<?php

namespace App\Http\Resources;

use Illuminate\Http\Request;
use Illuminate\Http\Resources\Json\JsonResource;

/**
 * Transforms a Submission model into the API JSON response shape.
 * Using API Resources decouples the DB column names from the public API
 * contract, so we can rename columns without breaking clients.
 */
class SubmissionResource extends JsonResource
{
    public function toArray(Request $request): array
    {
        return [
            'id'              => $this->id,
            'form_id'         => $this->form_id,
            'idempotency_key' => $this->idempotency_key,
            'email'           => $this->email,
            'data'            => $this->data,
            'created_at'      => $this->created_at->toIso8601String(),
            'updated_at'      => $this->updated_at->toIso8601String(),
        ];
    }
}
