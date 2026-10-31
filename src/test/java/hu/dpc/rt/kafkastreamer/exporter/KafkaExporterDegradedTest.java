package hu.dpc.rt.kafkastreamer.exporter;

import io.camunda.zeebe.broker.exporter.context.ExporterConfiguration;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.Map;

import static hu.dpc.rt.kafkastreamer.exporter.KafkaExporter.DEGRADED_EVENT;
import static hu.dpc.rt.kafkastreamer.exporter.KafkaExporter.REASON_INVALID_CONFIG;
import static hu.dpc.rt.kafkastreamer.exporter.KafkaExporter.REASON_PRODUCER_UNAVAILABLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Exporter problems must never stop Zeebe from processing payments: the exporter degrades instead.
class KafkaExporterDegradedTest {

    private final Logger logger = mock(Logger.class);
    private final Controller controller = mock(Controller.class);
    private final KafkaExporter exporter = new KafkaExporter();

    @Test
    void invalidConfigurationDegradesInsteadOfStoppingTheBroker() {
        assertDoesNotThrow(() -> exporter.configure(context(Map.of("kafkaTopic", "zeebe-export"))));
        assertDoesNotThrow(() -> exporter.open(controller));

        verify(logger).error(eq(DEGRADED_EVENT), eq(REASON_INVALID_CONFIG), eq("-"), contains("kafkaUrl is required"), any(Exception.class));
        assertExportIsHeldBack();
    }

    @Test
    void aNonNumericArgIsReportedWithTheSettingName() {
        exporter.configure(context(Map.of("kafkaUrl", "kafka:9092", "kafkaTopicPartitions", "six")));

        verify(logger).error(eq(DEGRADED_EVENT), eq(REASON_INVALID_CONFIG), eq("-"), contains("kafkaTopicPartitions"), any(Exception.class));
    }

    @Test
    void anUnresolvableKafkaHostDegradesInsteadOfFailingThePartition() {
        exporter.configure(context(Map.of("kafkaUrl", "kafka.invalid:9092")));
        assertDoesNotThrow(() -> exporter.open(controller));

        verify(logger).error(eq(DEGRADED_EVENT), eq(REASON_PRODUCER_UNAVAILABLE), eq("-"), contains("bootstrap"), any(Exception.class));
        assertExportIsHeldBack();
    }

    @Test
    void repeatedExportAttemptsLogTheDegradedEventOncePerInterval() {
        exporter.configure(context(Map.of()));
        exporter.open(controller);

        Record<?> record = record();
        assertThrows(IllegalStateException.class, () -> exporter.export(record));
        assertThrows(IllegalStateException.class, () -> exporter.export(record));

        verify(logger, times(1)).error(eq(DEGRADED_EVENT), eq(REASON_INVALID_CONFIG), eq("3"), contains("kafkaUrl is required"));
    }

    @Test
    void closingADegradedExporterDoesNotFail() {
        exporter.configure(context(Map.of()));
        exporter.open(controller);

        assertDoesNotThrow(exporter::close);
    }

    private void assertExportIsHeldBack() {
        Record<?> record = record();
        assertThrows(IllegalStateException.class, () -> exporter.export(record));
        verify(controller, never()).updateLastExportedRecordPosition(anyLong());
    }

    private static Record<?> record() {
        Record<?> record = KafkaExporterClientTest.event(1L);
        when(record.getPartitionId()).thenReturn(3);
        return record;
    }

    private Context context(Map<String, Object> args) {
        Context context = mock(Context.class);
        when(context.getLogger()).thenReturn(logger);
        when(context.getConfiguration()).thenReturn(new ExporterConfiguration("kafka", args));
        return context;
    }
}
