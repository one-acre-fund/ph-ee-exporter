package hu.dpc.rt.kafkastreamer.exporter;

import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// The exported position may only move once Kafka has acknowledged the records before it.
class KafkaExporterTest {

    private static final int BULK_SIZE = new KafkaExporterConfiguration().bulk.size;

    private final Controller controller = mock(Controller.class);
    private final MockProducer<String, String> producer = new MockProducer<>(false, new StringSerializer(), new StringSerializer());
    private final KafkaExporter exporter = new KafkaExporter();

    @BeforeEach
    void openExporter() {
        Logger logger = mock(Logger.class);
        Context context = mock(Context.class, RETURNS_DEEP_STUBS);
        when(context.getLogger()).thenReturn(logger);
        KafkaExporterConfiguration configuration = new KafkaExporterConfiguration();
        configuration.kafkaUrl = "kafka:9092";
        when(context.getConfiguration().instantiate(KafkaExporterConfiguration.class)).thenReturn(configuration);
        exporter.configure(context);
        exporter.open(controller, new KafkaExporterClient(new KafkaExporterConfiguration(), producer, logger));
    }

    @Test
    void holdsThePositionAndRejectsNewRecordsUntilKafkaAcknowledges() {
        for (long position = 1; position <= BULK_SIZE; position++) {
            exporter.export(positioned(position));
        }
        producer.errorNext(new RuntimeException("broker unavailable"));

        Record<?> next = positioned(BULK_SIZE + 1L);
        assertThrows(IllegalStateException.class, () -> exporter.export(next));
        verify(controller, never()).updateLastExportedRecordPosition(anyLong());

        exporter.export(next);
        verify(controller).updateLastExportedRecordPosition(BULK_SIZE);
        assertEquals(2 * BULK_SIZE + 1, producer.history().size());
    }

    @Test
    void positionWaitsForRecordsResentAfterAFailedScheduledFlush() {
        exporter.export(positioned(1));
        exporter.export(positioned(2));
        producer.errorNext(new RuntimeException("broker unavailable"));
        scheduledFlush().run();
        verify(controller, never()).updateLastExportedRecordPosition(anyLong());

        exporter.export(positioned(3));
        verify(controller, never()).updateLastExportedRecordPosition(anyLong());

        scheduledFlush().run();
        verify(controller).updateLastExportedRecordPosition(3L);
    }

    @Test
    void closeConfirmsPendingRecordsAndClosesTheProducer() {
        exporter.export(positioned(1));
        exporter.export(positioned(2));

        exporter.close();

        verify(controller).updateLastExportedRecordPosition(2L);
        assertTrue(producer.closed());
    }

    private Runnable scheduledFlush() {
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(controller, atLeastOnce()).scheduleCancellableTask(any(), task.capture());
        return task.getValue();
    }

    private static Record<?> positioned(long position) {
        Record<?> event = KafkaExporterClientTest.event(position);
        when(event.getPosition()).thenReturn(position);
        return event;
    }
}
