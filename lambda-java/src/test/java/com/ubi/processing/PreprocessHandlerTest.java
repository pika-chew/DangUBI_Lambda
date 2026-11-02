package com.ubi.processing;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PreprocessHandlerTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private S3Client s3;
    private SqsClient sqs;
    private Context context;
    private Map<String, String> environment;
    private PreprocessHandler handler;
    private List<SendMessageBatchRequest> sent;

    @BeforeEach
    void setup() {
        s3 = mock(S3Client.class);
        sqs = mock(SqsClient.class);
        context = mock(Context.class);
        when(context.getRemainingTimeInMillis()).thenReturn(30000);
        environment = new HashMap<>(Map.of("QUEUE_URL", "https://example.test/queue",
                "PROCESSED_BUCKET", "test-processed", "MAX_MESSAGE_BYTES", "262144"));
        handler = new PreprocessHandler(s3, sqs, environment);
        sent = new ArrayList<>();
        when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenAnswer(invocation -> {
            SendMessageBatchRequest request = invocation.getArgument(0);
            sent.add(request);
            return SendMessageBatchResponse.builder().successful(request.entries().stream()
                    .map(entry -> SendMessageBatchResultEntry.builder().id(entry.id()).build()).toList()).build();
        });
    }

    private Map<String, Object> record(int sequence) {
        return record(sequence, "{\"ty\":4,\"ts\":1793240000000,\"pld\":{\"tid\":354,\"sp\":45,\"em\":\"28\"}}");
    }

    private Map<String, Object> record(int sequence, String body) {
        return record(sequence, body.getBytes(StandardCharsets.UTF_8));
    }

    private Map<String, Object> record(int sequence, byte[] body) {
        return new HashMap<>(Map.of("eventSourceARN", "arn:aws:kinesis:ap-northeast-2:000000000000:stream/test",
                "eventID", "shardId-000000000000:" + sequence,
                "kinesis", new HashMap<>(Map.of("sequenceNumber", Integer.toString(sequence),
                        "partitionKey", "vehicle-001", "approximateArrivalTimestamp", 1793240000L,
                        "data", Base64.getEncoder().encodeToString(body)))));
    }

    private Map<String, Object> invoke(List<Map<String, Object>> records) {
        return handler.handleRequest(Map.of("Records", records), context);
    }

    private void assertFailures(Map<String, Object> result, String... sequences) {
        assertEquals(Map.of("batchItemFailures", List.of(sequences).stream()
                .map(sequence -> Map.of("itemIdentifier", sequence)).toList()), result);
    }

    private List<String> bodies() {
        return sent.stream().flatMap(request -> request.entries().stream()).map(entry -> entry.messageBody()).toList();
    }

    @Test
    void preservesTripMicrotripAndGpsPayloads() throws Exception {
        List<Map<String, Object>> records = List.of(record(1, "{\"ty\":3,\"pld\":{\"tid\":354,\"dis\":2559}}"),
                record(2), record(3, "{\"ty\":2,\"pld\":[{\"lat\":37.5}]}"));
        assertFailures(invoke(records));
        for (int index = 0; index < records.size(); index++) {
            Map<?, ?> source = (Map<?, ?>) records.get(index).get("kinesis");
            JsonNode original = JSON.readTree(Base64.getDecoder().decode((String) source.get("data")));
            JsonNode message = JSON.readTree(bodies().get(index));
            assertEquals(original, message.get("payload"));
            assertEquals(1, message.get("schemaVersion").asInt());
            assertEquals("vehicle-001", message.path("source").path("partitionKey").asText());
        }
    }

    @Test
    void archivesExactUtf8BodyBeforeSending() throws Exception {
        List<byte[]> stored = new ArrayList<>();
        when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(invocation -> {
            PutObjectRequest request = invocation.getArgument(0);
            RequestBody body = invocation.getArgument(1);
            assertEquals("test-processed", request.bucket());
            assertEquals("*", request.ifNoneMatch());
            stored.add(body.contentStreamProvider().newStream().readAllBytes());
            return PutObjectResponse.builder().build();
        });
        assertFailures(invoke(List.of(record(1, "{\"note\":\"한글\"}"))));
        assertArrayEquals(bodies().getFirst().getBytes(StandardCharsets.UTF_8), stored.getFirst());
        var order = inOrder(s3, sqs);
        order.verify(s3).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        order.verify(sqs).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void batchesAtMostTenMessages() {
        assertFailures(invoke(IntStream.rangeClosed(1, 23).mapToObj(this::record).toList()));
        assertEquals(List.of(10, 10, 3), sent.stream().map(request -> request.entries().size()).toList());
    }

    @Test
    void batchLimitCountsUtf8Bytes() {
        String body = "{\"note\":\"" + "가".repeat(50000) + "\"}";
        assertFailures(invoke(List.of(record(1, body), record(2, body), record(3, body))));
        assertEquals(3, sent.size());
        assertTrue(sent.stream().allMatch(request -> request.entries().stream()
                .mapToInt(entry -> entry.messageBody().getBytes(StandardCharsets.UTF_8).length).sum() <= 262144));
    }

    @Test
    void partialSqsFailureRetriesOnlyUnconfirmedRecord() {
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder()
                .successful(SendMessageBatchResultEntry.builder().id("0").build(),
                        SendMessageBatchResultEntry.builder().id("2").build()).build());
        assertFailures(invoke(List.of(record(1), record(2), record(3))), "2");
    }

    @Test
    void sqsExceptionRetriesEntireUnconfirmedBatch() {
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(new RuntimeException("network"));
        assertFailures(invoke(List.of(record(1), record(2))), "1", "2");
    }

    @Test
    @SuppressWarnings("unchecked")
    void malformedDataDoesNotBlockNeighbors() {
        Map<String, Object> badBase64 = record(2);
        ((Map<String, Object>) badBase64.get("kinesis")).put("data", "%%%");
        assertFailures(invoke(List.of(record(1), badBase64, record(3, "{broken"), record(4))), "2", "3");
        assertEquals(2, bodies().size());
    }

    @Test
    void rejectsArraysDuplicateKeysNonfiniteTrailingJsonAndInvalidUtf8() {
        List<Map<String, Object>> records = new ArrayList<>();
        String[] invalid = {"[]", "{\"a\":1,\"a\":2}", "{\"a\":NaN}", "{\"a\":Infinity}", "{} {}"};
        for (int index = 0; index < invalid.length; index++) {
            records.add(record(index + 1, invalid[index]));
        }
        records.add(record(6, new byte[]{(byte) 0xff}));
        records.add(record(7, "{\"a\":1}".getBytes(StandardCharsets.UTF_16)));
        assertFailures(invoke(records), "1", "2", "3", "4", "5", "6", "7");
        verifyNoInteractions(s3, sqs);
    }

    @Test
    void messageLimitIncludesMetadata() throws Exception {
        Map<String, Object> item = record(1);
        environment.put("MAX_MESSAGE_BYTES", Integer.toString(
                PreprocessHandler.messageBody(item).getBytes(StandardCharsets.UTF_8).length - 1));
        assertFailures(invoke(List.of(item)), "1");
        verifyNoInteractions(s3, sqs);
    }

    @Test
    void timeBudgetDefersWithoutWrites() {
        when(context.getRemainingTimeInMillis()).thenReturn(1000);
        assertFailures(invoke(List.of(record(1), record(2))), "1", "2");
        verifyNoInteractions(s3, sqs);
    }

    @Test
    void timeBudgetAfterArchiveDefersSqsSend() {
        when(context.getRemainingTimeInMillis()).thenReturn(30000, 1000);
        assertFailures(invoke(List.of(record(1))), "1");
        verify(s3).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verifyNoInteractions(sqs);
    }

    @Test
    void identityIsStableAndDistinctAcrossRecords() throws Exception {
        String first = PreprocessHandler.messageBody(record(1));
        assertEquals(first, PreprocessHandler.messageBody(record(1)));
        assertNotEquals(JSON.readTree(first).get("idempotencyKey"),
                JSON.readTree(PreprocessHandler.messageBody(record(2))).get("idempotencyKey"));
        assertEquals(64, JSON.readTree(first).get("idempotencyKey").asText().length());
        assertEquals("e0b73fd498fce0b4eaceae376d06d3d3a189e9ffdd11fda4d26583df9f0a9462",
                JSON.readTree(first).get("idempotencyKey").asText());
    }

    @Test
    void missingSqsSuccessIsFailure() {
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
        assertFailures(invoke(List.of(record(1))), "1");
    }

    @Test
    void missingSequenceFailsWholeInvocationBeforeWrites() {
        Map<String, Object> item = record(2);
        ((Map<?, ?>) item.get("kinesis")).remove("sequenceNumber");
        assertThrows(IllegalArgumentException.class, () -> invoke(List.of(record(1), item)));
        verifyNoInteractions(s3, sqs);
    }

    @Test
    void emptyBatchMakesNoAwsCalls() {
        assertFailures(invoke(List.of()));
        verifyNoInteractions(s3, sqs);
    }

    @Test
    void archiveFailureDoesNotSendFailedRecord() {
        when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(new RuntimeException("storage")).thenReturn(PutObjectResponse.builder().build());
        assertFailures(invoke(List.of(record(1), record(2))), "1");
        assertEquals(1, bodies().size());
    }

    @Test
    void existingArchiveAllowsRetryButConflictDoesNot() {
        when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(S3Exception.builder().statusCode(412)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("PreconditionFailed").build()).build())
                .thenThrow(S3Exception.builder().statusCode(409)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("ConditionalRequestConflict").build()).build());
        assertFailures(invoke(List.of(record(1))));
        assertFailures(invoke(List.of(record(2))), "2");
        assertEquals(1, bodies().size());
    }

    @Test
    void archiveKeyIsStableOnSqsRetry() {
        List<String> keys = new ArrayList<>();
        when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(invocation -> {
            keys.add(((PutObjectRequest) invocation.getArgument(0)).key());
            return PutObjectResponse.builder().build();
        });
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenThrow(new RuntimeException("network"))
                .thenReturn(SendMessageBatchResponse.builder()
                        .successful(SendMessageBatchResultEntry.builder().id("0").build()).build());
        assertFailures(invoke(List.of(record(1))), "1");
        assertFailures(invoke(List.of(record(1))));
        assertEquals(keys.getFirst(), keys.getLast());
    }

    @Test
    void logsDoNotExposePayloadOrSdkExceptionMessages() {
        when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(new RuntimeException("secret-sdk-message"));
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(capture, true, StandardCharsets.UTF_8));
            assertFailures(invoke(List.of(record(1, "{\"note\":\"private-car-data\"}"))), "1");
        } finally {
            System.setOut(original);
        }
        assertFalse(capture.toString(StandardCharsets.UTF_8).contains("private-car-data"));
        assertFalse(capture.toString(StandardCharsets.UTF_8).contains("secret-sdk-message"));
    }
}
