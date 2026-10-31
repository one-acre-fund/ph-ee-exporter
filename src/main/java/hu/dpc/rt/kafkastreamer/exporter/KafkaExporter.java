package hu.dpc.rt.kafkastreamer.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import org.slf4j.Logger;

import java.time.Duration;

/**
 * Bad configuration or an unreachable Kafka must not stop payments: Zeebe stops a broker whose
 * exporter throws from configure() and steps a partition down when open() throws. Instead the
 * exporter degrades: export() throws, so Zeebe keeps processing, holds the exported position and
 * retries the record, while {@link #DEGRADED_EVENT} is logged for alerting.
 */
public class KafkaExporter implements Exporter {
    // Alert on this. Format: event=kafka_exporter_degraded reason=<reason> partition=<id|-> detail="<text>"
    static final String DEGRADED_EVENT = "event=kafka_exporter_degraded reason={} partition={} detail=\"{}\"";
    static final String REASON_INVALID_CONFIG = "invalid_config";
    static final String REASON_PRODUCER_UNAVAILABLE = "producer_unavailable";
    private static final long DEGRADED_LOG_INTERVAL_MS = Duration.ofMinutes(1).toMillis();

    private Logger logger;
    private Controller controller;
    private KafkaExporterConfiguration configuration;
    private KafkaExporterClient client;
    private String configurationProblem;
    private long lastDegradedLogMs;

    private long lastPosition = -1;

    @Override
    public void configure(final Context context) {
        this.logger = context.getLogger();
        try {
            configuration = context.getConfiguration().instantiate(KafkaExporterConfiguration.class);
            configuration.validate();
            logger.info("DPC Kafka exporter configured with {}", configuration);
        } catch (Exception e) {
            configurationProblem = configurationMessage(e);
            logDegraded(REASON_INVALID_CONFIG, "-", configurationProblem, e);
        }
    }

    @Override
    public void open(final Controller controller) {
        logger.info("DPC Kafka exporter opening");
        this.controller = controller;
        if (configurationProblem == null) {
            connect("-");
            scheduleDelayedFlush();
        }
        logger.info("DPC Kafka exporter opened");
    }

    void open(final Controller controller, final KafkaExporterClient client) {
        this.controller = controller;
        this.client = client;
        scheduleDelayedFlush();
    }

    @Override
    public void close() {
        logger.info("DPC Kafka exporter closing");
        if (client == null) {
            logger.info("DPC Kafka exporter closed");
            return;
        }
        try {
            flush();
        } catch (final Exception e) {
            logger.warn("Failed to flush records before closing exporter.", e);
        }

        try {
            client.close();
        } catch (final Exception e) {
            logger.warn("Failed to close Kafka client", e);
        }

        logger.info("DPC Kafka exporter closed");
    }

    @Override
    public void export(Record<?> record) {
        logger.trace("Exporting record " + record);
        String partition = String.valueOf(record.getPartitionId());
        if (configurationProblem != null) {
            logDegradedEveryInterval(REASON_INVALID_CONFIG, partition, configurationProblem);
            throw new IllegalStateException("Kafka exporter is degraded, invalid configuration: " + configurationProblem);
        }
        if (client == null && !connect(partition)) {
            throw new IllegalStateException("Kafka exporter is degraded, Kafka producer could not be created");
        }
        // Zeebe retries a record whose export throws, so this holds new records back until Kafka catches up.
        if (client.shouldFlush() && !flush()) {
            throw new IllegalStateException("Kafka has not acknowledged the pending records yet");
        }
        client.index(record);
        lastPosition = record.getPosition();
        logger.trace("Finish exporting record " + record);
    }

    // A producer can fail to start (for example an unresolvable host before Kafka's DNS exists), so export retries it.
    private boolean connect(String partition) {
        try {
            client = new KafkaExporterClient(configuration, logger);
            return true;
        } catch (Exception e) {
            logDegradedEveryInterval(REASON_PRODUCER_UNAVAILABLE, partition, rootMessage(e), e);
            return false;
        }
    }

    private void logDegradedEveryInterval(String reason, String partition, String detail) {
        logDegradedEveryInterval(reason, partition, detail, null);
    }

    private void logDegradedEveryInterval(String reason, String partition, String detail, Exception cause) {
        long now = System.currentTimeMillis();
        if (lastDegradedLogMs == 0 || now - lastDegradedLogMs >= DEGRADED_LOG_INTERVAL_MS) {
            logDegraded(reason, partition, detail, cause);
            lastDegradedLogMs = now;
        }
    }

    private void logDegraded(String reason, String partition, String detail, Exception cause) {
        String singleLine = detail == null ? "" : detail.replace('"', '\'').replace('\n', ' ').replace('\r', ' ');
        if (cause == null) {
            logger.error(DEGRADED_EVENT, reason, partition, singleLine);
        } else {
            logger.error(DEGRADED_EVENT, reason, partition, singleLine, cause);
        }
    }

    // Our own messages are plain IllegalArgumentExceptions; Zeebe and Jackson wrap them, and
    // NumberFormatException (a subclass) would lose the setting name.
    private static String configurationMessage(Throwable e) {
        String message = e.getMessage();
        for (Throwable t = e; t != null && t.getCause() != t; t = t.getCause()) {
            if (t.getClass() == IllegalArgumentException.class && t.getMessage() != null) {
                message = t.getMessage();
            }
        }
        return message != null ? message : e.getClass().getSimpleName();
    }

    private static String rootMessage(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage() != null ? root.getMessage() : root.getClass().getSimpleName();
    }

    private void flushAndReschedule() {
        try {
            if (client != null) {
                flush();
            }
        } catch (final Exception e) {
            logger.error("Unexpected exception occurred on periodically flushing bulk, will retry later.", e);
        }
        scheduleDelayedFlush();
    }

    private void scheduleDelayedFlush() {
        controller.scheduleCancellableTask(Duration.ofSeconds(configuration.bulk.delay), this::flushAndReschedule);
    }

    private boolean flush() {
        if (client.flush()) {
            controller.updateLastExportedRecordPosition(lastPosition);
            return true;
        }
        logger.warn("Failed to flush bulk completely, keeping the last exported position");
        return false;
    }
}
