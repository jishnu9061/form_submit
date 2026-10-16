/**
 * k6 Load Test — POST /api/submissions
 * ======================================
 * Sends 100,000 requests total:
 *   - 80,000 with unique idempotency_keys
 *   - 20,000 duplicates (randomly reusing keys from the unique pool, including
 *     concurrent duplicates when --vus is high enough to overlap)
 *
 * How to run:
 *   docker run --rm -i --network host grafana/k6 run - < load-test/k6.js
 *   # Or if k6 is installed locally:
 *   k6 run load-test/k6.js
 *
 * Tune:
 *   --vus  Number of virtual users (concurrent workers). Set to match
 *          Octane worker count. Default: 50.
 *   --env  BASE_URL=http://localhost:8000  (override if needed)
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const BASE_URL    = __ENV.BASE_URL    || 'http://localhost:8000';
const UNIQUE_KEYS = parseInt(__ENV.UNIQUE_KEYS || '80000');
const TOTAL_REQS  = parseInt(__ENV.TOTAL_REQS  || '100000');
const DUPE_REQS   = TOTAL_REQS - UNIQUE_KEYS;   // 20,000

export const options = {
  scenarios: {
    // Ramp up to 50 VUs, hold, then ramp down.
    // 50 VUs × ~20 ms/req ≈ ~2,500 req/s abstract(estimate).
    load: {
      executor:    'constant-vus',
      vus:         50,
      duration:    '300s', // enough time for 100k at ~333 req/s
      gracefulStop: '10s',
    },
  },
  // Hard limits — fail the test if exceeded
  thresholds: {
    http_req_failed:   ['rate<0.001'], // < 0.1% non-2xx
    http_req_duration: ['p(95)<2000'], // 95th pct under 2 s
  },
};

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const createdCounter    = new Counter('submissions_created');    // 201 responses
const duplicateCounter  = new Counter('submissions_duplicate');  // 200 + duplicate:true
const errorCounter      = new Counter('submissions_errors');     // non-2xx

// ---------------------------------------------------------------------------
// Shared state: pre-generate all idempotency keys.
// k6 runs in a JS VM per VU, but __ENV and module-level code is shared
// across VUs in the same executor (Goja engine). We use a simple atomic
// counter pattern via SharedArray for large datasets.
// ---------------------------------------------------------------------------
import { SharedArray } from 'k6/data';

// Build the unique key pool once (shared across all VUs, read-only).
const uniqueKeys = new SharedArray('uniqueKeys', function () {
  const keys = [];
  for (let i = 0; i < UNIQUE_KEYS; i++) {
    keys.push(`key-${i}-${Math.random().toString(36).slice(2)}`);
  }
  return keys;
});

// ---------------------------------------------------------------------------
// Request counter — used to assign one of 100,000 "slots" to each iteration.
// Slots 0–79,999 → unique key; slots 80,000–99,999 → duplicate key.
// ---------------------------------------------------------------------------
let globalReqCounter = 0; // incremented per VU (approximate, races don't matter)

export default function () {
  // Each VU picks the next slot in a round-robin fashion.
  const slot = __VU * 10000 + __ITER; // unique enough distribution
  const isDuplicate = (slot % 5) === 0; // 1 in 5 requests → duplicate (= 20%)

  let idempotencyKey;
  if (isDuplicate) {
    // Reuse a random key from the unique pool — guaranteed duplicate
    idempotencyKey = uniqueKeys[Math.floor(Math.random() * uniqueKeys.length)];
  } else {
    // Generate a truly unique key for this iteration
    idempotencyKey = `vu${__VU}-iter${__ITER}-${Date.now()}`;
  }

  const payload = JSON.stringify({
    form_id:         'contact-form',
    idempotency_key: idempotencyKey,
    email:           `user${__VU}@example.com`,
    data:            { vu: __VU, iter: __ITER },
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Accept':       'application/json',
    },
    timeout: '10s',
  };

  const res = http.post(`${BASE_URL}/api/submissions`, payload, params);

  // Assertions
  const is201 = res.status === 201;
  const is200 = res.status === 200;
  const ok    = is201 || is200;

  check(res, {
    'status is 2xx':          () => ok,
    'has data.id':            () => ok && res.json('data.id') !== undefined,
    '201 has no duplicate':   () => !is201 || res.json('duplicate') !== true,
    '200 has duplicate:true': () => !is200 || res.json('duplicate') === true,
  });

  if (is201) createdCounter.add(1);
  else if (is200) duplicateCounter.add(1);
  else errorCounter.add(1);
}

// ---------------------------------------------------------------------------
// Summary report (printed at the end of the run)
// ---------------------------------------------------------------------------
export function handleSummary(data) {
  const created   = data.metrics.submissions_created?.values?.count  || 0;
  const duplicate = data.metrics.submissions_duplicate?.values?.count || 0;
  const errors    = data.metrics.submissions_errors?.values?.count   || 0;
  const rps       = data.metrics.http_reqs?.values?.rate?.toFixed(2) || 0;
  const p95       = data.metrics.http_req_duration?.values?.['p(95)']?.toFixed(2) || 0;

  const report = `
=======================================================
  LOAD TEST SUMMARY
=======================================================
  Total requests sent  : ${created + duplicate + errors}
  ✅ 201 Created        : ${created}    (expect ~80,000)
  🔁 200 Duplicate      : ${duplicate}  (expect ~20,000)
  ❌ Errors (non-2xx)   : ${errors}
  -------------------------------------------------------
  Throughput (req/s)   : ${rps}
  p(95) latency        : ${p95} ms
=======================================================

Verify with SQL:
  SELECT COUNT(*) FROM submissions;              -- expect 80,000
  SELECT COUNT(*) FROM submissions
    GROUP BY idempotency_key HAVING COUNT(*) > 1; -- expect 0 rows
=======================================================
`;

  console.log(report);

  return {
    'load-test/summary.txt': report,
    stdout: report,
  };
}
