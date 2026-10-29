package hu.dpc.rt.kafkastreamer.exporter;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.value.MessageRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.camunda.zeebe.protocol.record.value.DeploymentRecordValue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
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

    @SuppressWarnings("unchecked")
    private static Record<?> mockRecord(RecordValue value, long key) {
        Record<RecordValue> mocked = mock(Record.class);
        when(mocked.getKey()).thenReturn(key);
        when(mocked.getValue()).thenReturn(value);
        return mocked;
    }
}
