<?php

use App\Http\Controllers\Api\StatsController;
use App\Http\Controllers\Api\SubmissionController;
use Illuminate\Support\Facades\Route;

/*
|--------------------------------------------------------------------------
| API Routes
|--------------------------------------------------------------------------
*/

Route::post('/submissions', [SubmissionController::class, 'store'])
    ->name('submissions.store');

Route::get('/forms/{form_id}/submissions', [SubmissionController::class, 'index'])
    ->name('submissions.index');

Route::get('/forms/{form_id}/stats', [StatsController::class, 'hourly'])
    ->name('forms.stats');
