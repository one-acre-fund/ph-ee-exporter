package hu.dpc.rt.kafkastreamer.exporter;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.value.MessageRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.value.DeploymentRecordValue;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Records of one process instance must share a Kafka key so they stay on one partition.
class KafkaExporterClientTest {

    private final Logger logger = mock(Logger.class);

    @Test
    void keysByProcessInstanceWhenTheValueHasOne() {
        ProcessInstanceRecordValue value = mock(ProcessInstanceRecordValue.class);
        when(value.getProcessInstanceKey()).thenReturn(4503599629614132L);
        assertEquals("4503599629614132", KafkaExporterClient.extractKafkaKey(mockRecord(value, 2251799813685249L), logger));
    }

    @Test
    void fallsBackToTheRecordKeyWithoutAnInstance() {
        ProcessInstanceRecordValue value = mock(ProcessInstanceRecordValue.class);
        when(value.getProcessInstanceKey()).thenReturn(-1L);
        assertEquals("77", KafkaExporterClient.extractKafkaKey(mockRecord(value, 77L), logger));

        DeploymentRecordValue deployment = mock(DeploymentRecordValue.class);
        assertEquals("78", KafkaExporterClient.extractKafkaKey(mockRecord(deployment, 78L), logger));
    }

    @Test
    void messagesKeepTheRecordKey() {
        MessageRecordValue value = mock(MessageRecordValue.class);
        assertEquals("79", KafkaExporterClient.extractKafkaKey(mockRecord(value, 79L), logger));
    }

    @Test
    void sendsIndexedRecordsAsJsonKeyedByProcessInstance() {
        MockProducer<String, String> producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        ProcessInstanceRecordValue value = mock(ProcessInstanceRecordValue.class);
        when(value.getProcessInstanceKey()).thenReturn(4503599629614132L);
        Record<?> event = mockRecord(value, 2251799813685249L, RecordType.EVENT, ValueType.PROCESS_INSTANCE);
        when(event.toJson()).thenReturn("{\"key\":2251799813685249}");

        new KafkaExporterClient(new KafkaExporterConfiguration(), producer, logger).index(event);

        assertEquals(1, producer.history().size());
        ProducerRecord<String, String> sent = producer.history().get(0);
        assertEquals("zeebe-export", sent.topic());
        assertEquals("4503599629614132", sent.key());
        assertEquals("{\"key\":2251799813685249}", sent.value());
    }

    @Test
    void skipsRecordsThatAreNotIndexed() {
        MockProducer<String, String> producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        Record<?> command = mockRecord(mock(ProcessInstanceRecordValue.class), 80L, RecordType.COMMAND, ValueType.PROCESS_INSTANCE);

        new KafkaExporterClient(new KafkaExporterConfiguration(), producer, logger).index(command);

        assertTrue(producer.history().isEmpty());
    }

    @Test
    void resendsFromTheFirstUnacknowledgedRecordOnTheNextFlush() {
        MockProducer<String, String> producer = new MockProducer<>(false, new StringSerializer(), new StringSerializer());
        KafkaExporterClient client = new KafkaExporterClient(new KafkaExporterConfiguration(), producer, logger);
        client.index(event(1L));
        client.index(event(2L));
        client.index(event(3L));

        producer.completeNext();
        producer.errorNext(new RuntimeException("broker unavailable"));

        assertFalse(client.flush());
        assertEquals(List.of("1", "2", "3"), sentKeys(producer));

        assertTrue(client.flush());
        assertEquals(List.of("1", "2", "3", "2", "3"), sentKeys(producer));
        assertTrue(client.flush());
    }

    @Test
    void flushIsConfirmedWhenKafkaAcknowledgesEveryRecord() {
        MockProducer<String, String> producer = new MockProducer<>(false, new StringSerializer(), new StringSerializer());
        KafkaExporterClient client = new KafkaExporterClient(new KafkaExporterConfiguration(), producer, logger);
        client.index(event(1L));
        client.index(event(2L));

        assertTrue(client.flush());
        assertEquals(List.of("1", "2"), sentKeys(producer));
    }

    @Test
    void dropsARecordKafkaWillNeverAcceptInsteadOfBlockingExport() {
        MockProducer<String, String> producer = new MockProducer<>(false, new StringSerializer(), new StringSerializer());
        KafkaExporterClient client = new KafkaExporterClient(new KafkaExporterConfiguration(), producer, logger);
        client.index(event(1L));
        client.index(event(2L));

        producer.errorNext(new RecordTooLargeException("too large"));

        assertTrue(client.flush());
        assertEquals(List.of("1", "2"), sentKeys(producer));
    }

    @Test
    @SuppressWarnings("unchecked")
    void holdsRecordsBackAfterAnImmediateFailureInsteadOfBlockingOnEachSend() {
        Producer<String, String> producer = mock(Producer.class);
        when(producer.send(any())).thenReturn(failed(new TimeoutException("Topic zeebe-export not present in metadata after 5000 ms.")), acknowledged());
        KafkaExporterClient client = new KafkaExporterClient(new KafkaExporterConfiguration(), producer, logger);
        client.index(event(1L));
        client.index(event(2L));
        client.index(event(3L));
        verify(producer, times(1)).send(any());

        assertFalse(client.flush());
        verify(producer, times(1)).send(any());
        assertTrue(client.lastFailure().contains("not present in metadata"), client.lastFailure());

        assertTrue(client.flush());
        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer, times(4)).send(sent.capture());
        assertEquals(List.of("1", "1", "2", "3"), sent.getAllValues().stream().map(ProducerRecord::key).toList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void aTooLargeRecordDoesNotHoldBackTheRecordsAfterIt() {
        Producer<String, String> producer = mock(Producer.class);
        when(producer.send(any())).thenReturn(failed(new RecordTooLargeException("too large")), acknowledged());
        KafkaExporterClient client = new KafkaExporterClient(new KafkaExporterConfiguration(), producer, logger);
        client.index(event(1L));
        client.index(event(2L));

        verify(producer, times(2)).send(any());
        assertTrue(client.flush());
    }

    @Test
    void createsTheTopicAndClosesTheAdminClientWithoutWaiting() {
        Admin admin = adminAnswering(KafkaFuture.completedFuture(null));

        client().createTopic(admin, new NewTopic("zeebe-export", Optional.of(6), Optional.empty()), 100);

        verify(logger).info("created kafka topic {} with partitions {} and replication factor {}", "zeebe-export", 6, (short) -1);
        verify(admin).close(Duration.ZERO);
    }

    @Test
    void anExistingTopicIsNotAFailure() {
        Admin admin = adminAnswering(failedKafkaFuture(new TopicExistsException("exists")));

        client().createTopic(admin, new NewTopic("zeebe-export", Optional.empty(), Optional.empty()), 100);

        verify(logger).info("kafka topic {} already exists", "zeebe-export");
        verify(admin).close(Duration.ZERO);
    }

    @Test
    void aTopicThatCannotBeCreatedIsReported() {
        Admin admin = adminAnswering(failedKafkaFuture(new InvalidReplicationFactorException("only 1 broker")));

        client().createTopic(admin, new NewTopic("zeebe-export", Optional.empty(), Optional.of((short) 3)), 100);

        verify(logger).warn(eq("Failed to create Kafka topic {}"), eq("zeebe-export"), any(InvalidReplicationFactorException.class));
        verify(admin).close(Duration.ZERO);
    }

    @Test
    void anUnansweredCreateRequestGivesUpAfterTheTimeout() {
        Admin admin = adminAnswering(new KafkaFutureImpl<>());

        client().createTopic(admin, new NewTopic("zeebe-export", Optional.empty(), Optional.empty()), 10);

        verify(logger).warn("Kafka did not answer the create request for topic {} within {} ms", "zeebe-export", 10L);
        verify(admin).close(Duration.ZERO);
    }

    private KafkaExporterClient client() {
        return new KafkaExporterClient(new KafkaExporterConfiguration(), mock(Producer.class), logger);
    }

    private static Admin adminAnswering(KafkaFuture<Void> result) {
        Admin admin = mock(Admin.class);
        CreateTopicsResult created = mock(CreateTopicsResult.class);
        when(created.all()).thenReturn(result);
        when(admin.createTopics(any())).thenReturn(created);
        return admin;
    }

    private static KafkaFuture<Void> failedKafkaFuture(Exception cause) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.completeExceptionally(cause);
        return future;
    }

    private static Future<RecordMetadata> failed(Exception cause) {
        return CompletableFuture.failedFuture(cause);
    }

    private static Future<RecordMetadata> acknowledged() {
        return CompletableFuture.completedFuture(null);
    }

    static Record<?> event(long processInstanceKey) {
        ProcessInstanceRecordValue value = mock(ProcessInstanceRecordValue.class);
        when(value.getProcessInstanceKey()).thenReturn(processInstanceKey);
        Record<?> event = mockRecord(value, processInstanceKey, RecordType.EVENT, ValueType.PROCESS_INSTANCE);
        when(event.toJson()).thenReturn("{}");
        return event;
    }

    private static List<String> sentKeys(MockProducer<String, String> producer) {
        return producer.history().stream().map(ProducerRecord::key).toList();
    }

    private static Record<?> mockRecord(RecordValue value, long key, RecordType recordType, ValueType valueType) {
        Record<?> mocked = mockRecord(value, key);
        when(mocked.getRecordType()).thenReturn(recordType);
        when(mocked.getValueType()).thenReturn(valueType);
        return mocked;
    }

    @SuppressWarnings("unchecked")
    private static Record<?> mockRecord(RecordValue value, long key) {
        Record<RecordValue> mocked = mock(Record.class);
        when(mocked.getKey()).thenReturn(key);
        when(mocked.getValue()).thenReturn(value);
        return mocked;
    }
}
