<?php

namespace App\Http\Requests;

use Illuminate\Contracts\Validation\Validator;
use Illuminate\Foundation\Http\FormRequest;
use Illuminate\Http\Exceptions\HttpResponseException;

/**
 * Validates the POST /api/submissions request body.
 *
 * Why override failedValidation?
 *   Laravel's default validation failure returns HTTP 422 (Unprocessable Entity).
 *   The spec requires HTTP 400 (Bad Request), so we intercept the failed
 *   validator and throw an HttpResponseException with status 400 ourselves.
 *   This keeps the controller clean — it never sees invalid data.
 */
class StoreSubmissionRequest extends FormRequest
{
    public function authorize(): bool
    {
        return true; // public endpoint — no auth needed
    }

    public function rules(): array
    {
        return [
            'form_id'         => ['required', 'string', 'max:100'],
            'idempotency_key' => ['required', 'string', 'max:100'],
            'email'           => ['required', 'email:rfc,dns'],
            'data'            => ['required', 'array'],
        ];
    }

    /**
     * Override to return HTTP 400 instead of 422.
     */
    protected function failedValidation(Validator $validator): never
    {
        throw new HttpResponseException(
            response()->json([
                'message' => 'Validation failed.',
                'errors'  => $validator->errors(),
            ], 400)
        );
    }
}
