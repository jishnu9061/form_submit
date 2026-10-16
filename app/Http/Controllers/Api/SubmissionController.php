<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Http\Requests\StoreSubmissionRequest;
use App\Http\Resources\SubmissionResource;
use App\Models\Submission;
use App\Services\SubmissionService;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Http\Resources\Json\AnonymousResourceCollection;

/**
 * SubmissionController — thin HTTP layer only.
 * All business logic is delegated to SubmissionService.
 */
class SubmissionController extends Controller
{
    public function __construct(
        private readonly SubmissionService $service,
    ) {}

    /**
     * POST /api/submissions
     *
     * Returns 201 for a new record, 200 for a duplicate (with duplicate:true flag).
     */
    public function store(StoreSubmissionRequest $request): JsonResponse
    {
        $result = $this->service->createOrFindDuplicate(
            formId:         $request->validated('form_id'),
            idempotencyKey: $request->validated('idempotency_key'),
            email:          $request->validated('email'),
            data:           $request->validated('data'),
        );

        $status = $result['is_duplicate'] ? 200 : 201;

        return (new SubmissionResource($result['submission']))
            ->additional($result['is_duplicate'] ? ['duplicate' => true] : [])
            ->response()
            ->setStatusCode($status);
    }

    /**
     * GET /api/forms/{form_id}/submissions
     *
     * Pagination tradeoff:
     *   simplePaginate() (cursor-style) avoids SELECT COUNT(*) and deep OFFSET cost.
     *   Regular paginate() scans all rows to count them — O(n) on large tables and
     *   kills throughput at page 10,000. We use simplePaginate() here because:
     *     1. The (form_id, id) composite index means the WHERE + ORDER BY is fast.
     *     2. Clients that need "jump to page N" can pass an ?after_id cursor instead.
     *   Trade: the response has no total count in meta. That's acceptable for feeds;
     *   if total count is needed, use paginate() or cache the count separately.
     */
    public function index(Request $request, string $formId): AnonymousResourceCollection
    {
        $submissions = Submission::where('form_id', $formId)
            ->orderBy('id', 'desc')
            ->simplePaginate(10);

        return SubmissionResource::collection($submissions);
    }
}
