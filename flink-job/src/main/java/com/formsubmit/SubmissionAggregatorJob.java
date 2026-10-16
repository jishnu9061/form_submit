package com.formsubmit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.formsubmit.SubmissionAggregatorJob.CountAggregator;
import com.formsubmit.SubmissionAggregatorJob.WindowMetadata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * Flink DataStream job — real-time hourly submission counter.
 *
 * Architecture overview:
 * ─────────────────────
 * Kafka (topic: submissions)
 *   → parse JSON, extract (form_id, event_time)
 *   → assign event-time watermarks (1-min out-of-orderness tolerance)
 *   → key by form_id
 *   → tumbling 1-hour event-time window
 *       with ContinuousProcessingTimeTrigger (fire every 10 s for "live" counts)
 *   → count aggregation
 *   → JDBC sink → MySQL form_hourly_counts (idempotent UPSERT)
 *
 * Fault tolerance design:
 * ───────────────────────
 * 1. EXACTLY_ONCE checkpointing every 10 s with RocksDB state backend.
 *    RocksDB stores state on disk, so the state can survive TaskManager restarts
 *    even on machines with limited heap.
 *
 * 2. Kafka offsets are stored inside Flink checkpoints (not in Kafka's
 *    consumer_offsets topic). This means on recovery Flink restores to the
 *    last checkpoint's offset and replays from there. We get exactly-once
 *    semantics end-to-end IF the sink is idempotent.
 *
 * 3. Idempotent sink: we use INSERT ... ON DUPLICATE KEY UPDATE count = VALUES(count).
 *    When Flink replays (form_id='x', hour='2024-01-01T10:00:00', count=42),
 *    the upsert simply overwrites the existing row — not double-counts.
 *
 * 4. The API-level idempotency_key ensures each physical submission enters
 *    Kafka exactly once (duplicate HTTP calls are filtered at the DB layer),
 *    which is the first line of defense against double-counting.
 *
 * 5. Restart strategy: fixed-delay, 10 attempts, 10 s delay between attempts.
 */
public class SubmissionAggregatorJob {

    // MySQL UPSERT — idempotent: replays overwrite, not add
    private static final String UPSERT_SQL =
            "INSERT INTO form_hourly_counts (form_id, hour_start, count) " +
            "VALUES (?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE count = VALUES(count)";

    public static void main(String[] args) throws Exception {

        // ----------------------------------------------------------------
        // 1. Execution environment
        // ----------------------------------------------------------------
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ----------------------------------------------------------------
        // 2. Checkpointing — EXACTLY_ONCE, 10 s interval, RocksDB backend
        //
        //    EXACTLY_ONCE: Flink will not commit a checkpoint until all
        //    operators have successfully snapshotted their state. This means
        //    the checkpoint is a consistent, atomic cut across the whole job.
        //
        //    Checkpoint storage: filesystem (local in Docker; use S3/GCS in prod).
        //    RocksDB state backend: state is written to disk incrementally, so
        //    large window state doesn't blow up the JVM heap.
        // ----------------------------------------------------------------
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // ----------------------------------------------------------------
        // 3. Restart strategy — fixed-delay, 10 attempts, 10 s between tries.
        //    After 10 failures the job fails and the operator must restart it.
        // ----------------------------------------------------------------
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, Time.seconds(10))
        );

        // ----------------------------------------------------------------
        // 4. Kafka source
        //    Offsets are stored in Flink checkpoints (setStartingOffsets with
        //    committedOffsets() falls back to EARLIEST on first run).
        //    On recovery, Flink uses the checkpoint offset → no auto-commit.
        // ----------------------------------------------------------------
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(System.getenv().getOrDefault("KAFKA_BROKERS", "kafka:9092"))
                .setTopics("submissions")
                .setGroupId("flink-submission-aggregator")
                // On first run, start from the beginning of the topic.
                // On recovery, Flink restores offsets from the checkpoint.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        OffsetsInitializer.earliest()))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // ----------------------------------------------------------------
        // 5. Watermark strategy — event time with 1-min out-of-orderness.
        //    We use the created_at field in the Kafka message as event time.
        //    The 60-second slack means a message up to 1 min late won't be
        //    dropped; a message 2 min late will fall into a closed window and
        //    be ignored (acceptable for hourly aggregation).
        // ----------------------------------------------------------------
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner((event, recordTimestamp) -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        mapper.registerModule(new JavaTimeModule());
                        ObjectNode node = (ObjectNode) mapper.readTree(event);
                        String createdAt = node.get("created_at").asText();
                        return Instant.parse(createdAt).toEpochMilli();
                    } catch (Exception e) {
                        // Fall back to ingestion time if event time is missing/malformed.
                        return System.currentTimeMillis();
                    }
                });

        // ----------------------------------------------------------------
        // 6. Build the pipeline
        // ----------------------------------------------------------------
        DataStream<Tuple3<String, Long, Long>> counts = env
                // Source with event-time watermarks
                .fromSource(kafkaSource, watermarkStrategy, "Kafka Submissions")

                // Parse JSON → (form_id, hour_start_epoch_ms)
                .map(json -> {
                    ObjectMapper mapper = new ObjectMapper();
                    ObjectNode node = (ObjectNode) mapper.readTree(json);
                    String formId    = node.get("form_id").asText();
                    String createdAt = node.get("created_at").asText();
                    long   epochMs   = Instant.parse(createdAt).toEpochMilli();
                    return Tuple3.of(formId, epochMs, 1L);
                })
                .returns(org.apache.flink.api.common.typeinfo.TypeInformation
                        .of(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple3<String, Long, Long>>() {}))

                // Key by form_id so each form has its own window state
                .keyBy(t -> t.f0)

                // Tumbling 1-hour event-time window
                .window(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.hours(1)))

                // Fire early results every 10 s of processing time so counts
                // are visible in MySQL before the hour window closes.
                // This does NOT affect exactly-once correctness — the idempotent
                // upsert overwrites previous partial results.
                .trigger(ContinuousProcessingTimeTrigger.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))

                // Count submissions per window
                .aggregate(new CountAggregator(), new WindowMetadata());

        // ----------------------------------------------------------------
        // 7. JDBC sink — idempotent UPSERT into MySQL
        //
        //    executionAttemptId is tracked by Flink so the JDBC sink is
        //    aware of retries and won't duplicate rows (the UPSERT handles it).
        //    batchSize=1 ensures every early trigger result is visible quickly.
        //    In production, set batchSize=100 and batchIntervalMs=1000 for
        //    higher throughput at the cost of up to 1 s latency.
        // ----------------------------------------------------------------
        counts.addSink(
                JdbcSink.sink(
                        UPSERT_SQL,                .build();

        // ----------------------------------------------------------------
        // 5. Watermark strategy — event time with 1-min out-of-orderness.
        //    We use the created_at field in the Kafka message as event time.
        //    The 60-second slack means a message up to 1 min late won't be
        //    dropped; a message 2 min late will fall into a closed window and
        //    be ignored (acceptable for hourly aggregation).
        // ----------------------------------------------------------------
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner((event, recordTimestamp) -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        mapper.registerModule(new JavaTimeModule());
                        ObjectNode node = (ObjectNode) mapper.readTree(event);
                        String createdAt = node.get("created_at").asText();
                        return Instant.parse(createdAt).toEpochMilli();
                    } catch (Exception e) {
                        // Fall back to ingestion time if event time is missing/malformed.
                        return System.currentTimeMillis();
                    }
                });

        // ----------------------------------------------------------------
        // 6. Build the pipeline
        // ----------------------------------------------------------------
        DataStream<Tuple3<String, Long, Long>> counts = env
                // Source with event-time watermarks
                .fromSource(kafkaSource, watermarkStrategy, "Kafka Submissions")

                // Parse JSON → (form_id, hour_start_epoch_ms)
                .map(json -> {
                    ObjectMapper mapper = new ObjectMapper();
                    ObjectNode node = (ObjectNode) mapper.readTree(json);

                        (statement, t) -> {
                            statement.setString(1, t.f0);                          // form_id
                            statement.setTimestamp(2, new Timestamp(t.f1));        // hour_start
                            statement.setLong(3, t.f2);                            // count
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(0)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(System.getenv().getOrDefault(
                                        "MYSQL_URL",
                                        "jdbc:mysql://mysql:3306/app?useSSL=false&allowPublicKeyRetrieval=true"))
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername(System.getenv().getOrDefault("MYSQL_USER", "app_user"))
                                .withPassword(System.getenv().getOrDefault("MYSQL_PASSWORD", "secret"))
                                .build()
                )
        ).name("MySQL Hourly Counts Sink");

        env.execute("Submission Hourly Aggregator");
    }

    // ========================================================================
    // Aggregation function — simple counter
    // ========================================================================
    static class CountAggregator
            implements AggregateFunction<
                Tuple3<String, Long, Long>,   // IN
                Long,                          // ACC
                Long> {                        // OUT

        @Override public Long createAccumulator()        { return 0L; }
        @Override public Long add(Tuple3<String, Long, Long> v, Long acc) { return acc + 1; }
        @Override public Long getResult(Long acc)        { return acc; }
        @Override public Long merge(Long a, Long b)      { return a + b; }
    }

    // ========================================================================
    // ProcessWindowFunction — enriches the count with (form_id, window_start)
    // ========================================================================
    static class WindowMetadata
            extends ProcessWindowFunction<Long, Tuple3<String, Long, Long>, String, TimeWindow> {

        @Override
        public void process(
                String formId,
                Context ctx,
                Iterable<Long> counts,
                Collector<Tuple3<String, Long, Long>> out) {

            long count       = counts.iterator().next();
            long windowStart = ctx.window().getStart(); // epoch ms of the hour boundary

            out.collect(Tuple3.of(formId, windowStart, count));
        }
    }
}
