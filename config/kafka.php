<?php

return [
    /*
    |--------------------------------------------------------------------------
    | Kafka Brokers
    |--------------------------------------------------------------------------
    | Comma-separated list of broker addresses. Inside Docker Compose the app
    | container addresses Kafka by its service name on port 9092 (internal).
    | When running locally, use localhost:29092 (the mapped host port).
    */
    'brokers' => env('KAFKA_BROKERS', 'kafka:9092'),

    /*
    |--------------------------------------------------------------------------
    | Security / SASL
    |--------------------------------------------------------------------------
    | For local development we use plaintext. Update these for production SASL.
    */
    'security_protocol' => env('KAFKA_SECURITY_PROTOCOL', 'plaintext'),
];
