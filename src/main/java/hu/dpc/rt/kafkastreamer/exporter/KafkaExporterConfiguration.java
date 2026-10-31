package hu.dpc.rt.kafkastreamer.exporter;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Zeebe fills this from the exporter's {@code args} (matched case-insensitively, so
 * {@code ZEEBE_BROKER_EXPORTERS_KAFKA_ARGS_KAFKAURL} sets {@code kafkaUrl}). The constructor
 * first loads the {@code ZEEBE_KAFKAEXPORT_*} env vars, so args win and env is the fallback.
 */
public class KafkaExporterConfiguration {
    private static final int KAFKA_DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

    public String kafkaUrl;
    public String kafkaTopic;
    public Integer kafkaTopicPartitions;
    public Integer kafkaTopicReplicationFactor;
    // Unset leaves the Kafka producer defaults (30s and 120s).
    public Integer requestTimeoutMs;
    public Integer deliveryTimeoutMs;

    public BulkConfiguration bulk = new BulkConfiguration();


    public KafkaExporterConfiguration() {
        kafkaUrl = System.getenv("ZEEBE_KAFKAEXPORT_URL");
        kafkaTopic = System.getenv("ZEEBE_KAFKAEXPORT_TOPIC");
        if (ObjectUtils.isEmpty(kafkaTopic)) {
            kafkaTopic = "zeebe-export";
        }
        kafkaTopicPartitions = parseEnvInt("ZEEBE_KAFKAEXPORT_TOPIC_PARTITIONS");
        kafkaTopicReplicationFactor = parseEnvInt("ZEEBE_KAFKAEXPORT_TOPIC_REPLICATION_FACTOR");
    }

    private static Integer parseEnvInt(String key) {
        return parseInt(key, System.getenv(key));
    }

    static Integer parseInt(String key, String value) {
        if (ObjectUtils.isEmpty(value)) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a whole number, got '" + value + "'", e);
        }
    }

    /**
     * Rejects settings the exporter cannot run with, so the broker refuses to start
     * instead of failing later on the first export.
     */
    public void validate() {
        List<String> problems = new ArrayList<>();
        if (kafkaUrl == null || kafkaUrl.isBlank()) {
            problems.add("kafkaUrl is required (args.kafkaUrl or ZEEBE_KAFKAEXPORT_URL)");
        }
        if (kafkaTopic == null || kafkaTopic.isBlank()) {
            problems.add("kafkaTopic must not be empty");
        }
        requirePositive(problems, "kafkaTopicPartitions", kafkaTopicPartitions);
        requirePositive(problems, "kafkaTopicReplicationFactor", kafkaTopicReplicationFactor);
        requirePositive(problems, "requestTimeoutMs", requestTimeoutMs);
        requirePositive(problems, "deliveryTimeoutMs", deliveryTimeoutMs);
        requirePositive(problems, "bulk.size", bulk.size);
        requirePositive(problems, "bulk.delay", bulk.delay);
        if (deliveryTimeoutMs != null && deliveryTimeoutMs < effectiveRequestTimeoutMs()) {
            problems.add("deliveryTimeoutMs (" + deliveryTimeoutMs + ") must be at least requestTimeoutMs (" + effectiveRequestTimeoutMs() + ")");
        }
        if (!problems.isEmpty()) {
            throw new IllegalArgumentException("Invalid Kafka exporter configuration: " + String.join("; ", problems));
        }
    }

    private int effectiveRequestTimeoutMs() {
        return requestTimeoutMs != null ? requestTimeoutMs : KAFKA_DEFAULT_REQUEST_TIMEOUT_MS;
    }

    private static void requirePositive(List<String> problems, String name, Integer value) {
        if (value != null && value < 1) {
            problems.add(name + " must be at least 1, got " + value);
        }
    }

    @Override
    public String toString() {
        return "KafkaExporterConfiguration {" +
                "kafkaUrl='" + kafkaUrl + '\'' +
                ", kafkaTopic='" + kafkaTopic + '\'' +
                ", kafkaTopicPartitions='" + kafkaTopicPartitions + '\'' +
                ", kafkaTopicReplicationFactor='" + kafkaTopicReplicationFactor + '\'' +
                ", requestTimeoutMs=" + requestTimeoutMs +
                ", deliveryTimeoutMs=" + deliveryTimeoutMs +
                ", bulk=" + bulk +
                '}';
    }

    public boolean shouldIndexRecord(final Record<?> record) {
        return shouldIndexRecordType(record.getRecordType()) && shouldIndexValueType(record.getValueType());
    }

    public boolean shouldIndexRecordType(final RecordType recordType) {
        return switch (recordType) {
            case COMMAND -> false;
            case COMMAND_REJECTION -> false;
            case EVENT -> true;
            case NULL_VAL -> false;
            case SBE_UNKNOWN -> false;
        };
    }

    public boolean shouldIndexValueType(final ValueType valueType) {
        return switch (valueType) {
            case CHECKPOINT -> false;
            case COMMAND_DISTRIBUTION -> false;
            case DECISION -> false;
            case DECISION_EVALUATION -> false;
            case DECISION_REQUIREMENTS -> false;
            case DEPLOYMENT -> true;
            case DEPLOYMENT_DISTRIBUTION -> false;
            case ERROR -> true;
            case ESCALATION -> false;
            case INCIDENT -> true;
            case JOB -> true;
            case JOB_BATCH -> false;
            case MESSAGE -> true;
            case MESSAGE_START_EVENT_SUBSCRIPTION -> false;
            case MESSAGE_SUBSCRIPTION -> false;
            case NULL_VAL -> false;
            case PROCESS -> false;
            case PROCESS_EVENT -> true;
            case PROCESS_INSTANCE -> true;
            case PROCESS_INSTANCE_BATCH -> false;
            case PROCESS_INSTANCE_CREATION -> false;
            case PROCESS_INSTANCE_MODIFICATION -> false;
            case PROCESS_INSTANCE_RESULT -> false;
            case PROCESS_MESSAGE_SUBSCRIPTION -> false;
            case RESOURCE_DELETION -> false;
            case SBE_UNKNOWN -> false;
            case SIGNAL -> false;
            case SIGNAL_SUBSCRIPTION -> false;
            case TIMER -> true;
            case VARIABLE -> true;
            case VARIABLE_DOCUMENT -> true;
        };
    }

    public static class BulkConfiguration {
        // delay before forced flush
        public int delay = 5;
        // bulk size before flush
        public int size = 1_000;

        @Override
        public String toString() {
            return "BulkConfiguration{" + "delay=" + delay + ", size=" + size + '}';
        }
    }

    public boolean isMskEnabled() {
        return "true".equalsIgnoreCase(System.getenv("ZEEBE_MSK_ENABLED"));
    }
}
