package com.ubi.processing;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class PreprocessHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {
    private static final int MAX_BATCH_BYTES = 262144;
    private static final int MIN_REMAINING_MS = 12000;
    private static final ObjectMapper JSON = new ObjectMapper(
            JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final S3Client s3;
    private final SqsClient sqs;
    private final Map<String, String> environment;

    public PreprocessHandler() {
        this(Clients.S3, Clients.SQS, System.getenv());
    }

    PreprocessHandler(S3Client s3, SqsClient sqs, Map<String, String> environment) {
        this.s3 = s3;
        this.sqs = sqs;
        this.environment = environment;
    }

    private static final class Clients {
        private static final ClientOverrideConfiguration CONFIG = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofSeconds(10))
                .apiCallAttemptTimeout(Duration.ofSeconds(4))
                .retryStrategy(StandardRetryStrategy.builder().maxAttempts(2).build())
                .build();
        private static final S3Client S3 = S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(2)).socketTimeout(Duration.ofSeconds(3)))
                .overrideConfiguration(CONFIG).build();
        private static final SqsClient SQS = SqsClient.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(2)).socketTimeout(Duration.ofSeconds(3)))
                .overrideConfiguration(CONFIG).build();
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        String queueUrl = setting("QUEUE_URL");
        String bucket = setting("PROCESSED_BUCKET");
        int maxBytes = Math.min(Integer.parseInt(setting("MAX_MESSAGE_BYTES")), MAX_BATCH_BYTES);
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("Invalid message limit");
        }
        List<?> records = (List<?>) event.get("Records");
        List<String> sequences = new ArrayList<>();
        // A missing checkpoint identifier must fail the invocation before any writes.
        for (Object item : records) {
            sequences.add(text(object(object(item).get("kinesis")), "sequenceNumber"));
        }

        Set<String> failed = new HashSet<>();
        List<Pending> pending = new ArrayList<>();
        int pendingBytes = 0;
        for (int index = 0; index < records.size(); index++) {
            Map<String, Object> record = object(records.get(index));
            String sequence = sequences.get(index);
            String body;
            byte[] encoded;
            try {
                body = messageBody(record);
                encoded = body.getBytes(StandardCharsets.UTF_8);
                if (encoded.length > maxBytes) {
                    throw new IllegalArgumentException("Message exceeds queue limit");
                }
            } catch (Exception exception) {
                failed.add(sequence);
                log("record_rejected", "sequenceNumber", sequence, "error", exception.getClass().getSimpleName());
                continue;
            }

            if (!pending.isEmpty() && (pending.size() == 10 || pendingBytes + encoded.length > MAX_BATCH_BYTES)) {
                failed.addAll(send(pending, queueUrl, context));
                pending.clear();
                pendingBytes = 0;
            }
            if (context.getRemainingTimeInMillis() < MIN_REMAINING_MS) {
                failed.add(sequence);
                log("forward_deferred", "count", 1, "reason", "time_budget");
                continue;
            }
            try {
                archive(record, encoded, bucket);
            } catch (Exception exception) {
                failed.add(sequence);
                log("record_archive_failed", "sequenceNumber", sequence, "error", exception.getClass().getSimpleName());
                continue;
            }
            pending.add(new Pending(sequence, body));
            pendingBytes += encoded.length;
        }
        if (!pending.isEmpty()) {
            failed.addAll(send(pending, queueUrl, context));
        }
        log("processing_summary", "received", records.size(), "failed", failed.size());
        return Map.of("batchItemFailures", sequences.stream().filter(failed::contains)
                .map(sequence -> Map.of("itemIdentifier", sequence)).toList());
    }

    static String messageBody(Map<String, Object> record) throws Exception {
        Map<String, Object> source = object(record.get("kinesis"));
        byte[] decoded = Base64.getDecoder().decode(text(source, "data"));
        JsonNode payload = JSON.readTree(StandardCharsets.UTF_8.newDecoder()
                .decode(ByteBuffer.wrap(decoded)).toString());
        if (payload == null || !payload.isObject()) {
            throw new IllegalArgumentException("Expected a JSON object");
        }
        // Ordered maps keep serialized bytes stable across separate JVMs and retries.
        Map<String, Object> metadata = fields(
                "streamArn", text(record, "eventSourceARN"), "eventId", text(record, "eventID"),
                "partitionKey", text(source, "partitionKey"), "sequenceNumber", text(source, "sequenceNumber"),
                "approximateArrivalTimestamp", source.get("approximateArrivalTimestamp"));
        return JSON.writeValueAsString(fields(
                "schemaVersion", 1, "idempotencyKey", identity(record), "source", metadata, "payload", payload));
    }

    private void archive(Map<String, Object> record, byte[] body, String bucket) throws Exception {
        String identity = identity(record);
        String key = "processed/" + identity.substring(0, 2) + "/" + identity + "/" + hash(body) + ".json";
        try {
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                    .contentType("application/json; charset=utf-8").ifNoneMatch("*").build(),
                    RequestBody.fromBytes(body));
        } catch (S3Exception exception) {
            if (exception.statusCode() != 412 || exception.awsErrorDetails() == null
                    || !"PreconditionFailed".equals(exception.awsErrorDetails().errorCode())) {
                throw exception;
            }
        }
    }

    private Set<String> send(List<Pending> pending, String queueUrl, Context context) {
        Set<String> failed = new HashSet<>();
        pending.forEach(item -> failed.add(item.sequence()));
        if (context.getRemainingTimeInMillis() < MIN_REMAINING_MS) {
            log("forward_deferred", "count", pending.size(), "reason", "time_budget");
            return failed;
        }
        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int index = 0; index < pending.size(); index++) {
            entries.add(SendMessageBatchRequestEntry.builder().id(Integer.toString(index))
                    .messageBody(pending.get(index).body()).build());
        }
        try {
            var response = sqs.sendMessageBatch(SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl).entries(entries).build());
            for (var success : response.successful()) {
                for (int index = 0; index < pending.size(); index++) {
                    if (Integer.toString(index).equals(success.id())) {
                        failed.remove(pending.get(index).sequence());
                    }
                }
            }
            if (!failed.isEmpty()) {
                log("forward_batch_failed", "failed", failed.size());
            }
        } catch (Exception exception) {
            log("forward_batch_failed", "failed", failed.size(), "error", exception.getClass().getSimpleName());
        }
        return failed;
    }

    private String setting(String name) {
        String value = environment.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing setting: " + name);
        }
        return value;
    }

    private static String identity(Map<String, Object> record) throws Exception {
        return hash((text(record, "eventSourceARN") + ":" + text(record, "eventID")).getBytes(StandardCharsets.UTF_8));
    }

    private static String hash(byte[] value) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Expected an object");
        }
        return (Map<String, Object>) value;
    }

    private static String text(Map<String, Object> value, String key) {
        if (!(value.get(key) instanceof String result) || result.isEmpty()) {
            throw new IllegalArgumentException("Missing string field: " + key);
        }
        return result;
    }

    private static Map<String, Object> fields(Object... pairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            result.put((String) pairs[index], pairs[index + 1]);
        }
        return result;
    }

    private static void log(String event, Object... pairs) {
        Map<String, Object> record = fields(pairs);
        record.put("event", event);
        try {
            System.out.println(JSON.writeValueAsString(record));
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot serialize processing log");
        }
    }

    private record Pending(String sequence, String body) {}
}
