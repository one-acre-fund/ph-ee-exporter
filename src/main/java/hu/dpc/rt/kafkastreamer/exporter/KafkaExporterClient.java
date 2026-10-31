package hu.dpc.rt.kafkastreamer.exporter;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.value.MessageRecordValue;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaExporterClient {
    private Logger logger;
    private KafkaExporterMetrics metrics;
    // Records sent since the last confirmed flush, in send order.
    private final List<PendingRecord> pending = new ArrayList<>();
    private boolean initialized;

    private final Producer<String, String> producer;
    private final KafkaExporterConfiguration configuration;

    public KafkaExporterClient(KafkaExporterConfiguration configuration, Logger logger) {
        this.logger = logger;
        this.configuration = configuration;
        String clientId = buildKafkaClientId(logger);

        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.kafkaUrl);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        if (configuration.requestTimeoutMs != null) {
            properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, configuration.requestTimeoutMs);
        }
        if (configuration.deliveryTimeoutMs != null) {
            properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, configuration.deliveryTimeoutMs);
        }

        if (configuration.isMskEnabled()) {
            logger.info("configuring Kafka client with AWS MSK support");
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            properties.put(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, "software.amazon.msk.auth.iam.IAMLoginModule required;");
            properties.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        } else {
            logger.info("configuring Kafka client for plain Kafka without MSK support)");
        }

        this.producer = new KafkaProducer<>(properties);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(Arrays.asList(new NewTopic(configuration.kafkaTopic, Optional.ofNullable(configuration.kafkaTopicPartitions), Optional.ofNullable(configuration.kafkaTopicReplicationFactor).map(Integer::shortValue))));
            logger.info("created kafka topic {} with partitions {} and replication factor {} successfully", configuration.kafkaTopic, configuration.kafkaTopicPartitions, configuration.kafkaTopicReplicationFactor);
        } catch (Exception e) {
            logger.warn("Failed to create Kafka topic (it exists already?)", e);
        }

        logger.info("configured Kafka producer with client id {}", clientId);
    }

    KafkaExporterClient(KafkaExporterConfiguration configuration, Producer<String, String> producer, Logger logger) {
        this.configuration = configuration;
        this.producer = producer;
        this.logger = logger;
    }

    public static String buildKafkaClientId(Logger logger) {
        return UUID.randomUUID().toString();
    }

    public void close() {
        producer.close();
    }

    public void index(Record<?> zeebeRecord) {
        if (metrics == null && !initialized) {
            try {
                metrics = new KafkaExporterMetrics(zeebeRecord.getPartitionId());
            } catch (Exception e) {
                logger.warn("## failed to initialize metrics, continuing without it");
            }
            initialized = true;
        }

        if (configuration.shouldIndexRecord(zeebeRecord)) {
            String json = zeebeRecord.toJson();
            logger.trace("sending record to kafka: {}", json);
            if (metrics != null) {
                metrics.recordBulkSize(1);
            }
            String key = extractKafkaKey(zeebeRecord, logger);
            send(new ProducerRecord<>(configuration.kafkaTopic, key, json));
        } else {
            logger.trace("skipping record: {}", zeebeRecord);
        }
    }

    // Key by process instance so every record of one instance lands on one partition and one
    // importer replica sees them in order. Records without an instance (deployments, messages)
    // keep the record key. Taken from openMF/ph-ee-exporter v8.4.0-1.3.
    static String extractKafkaKey(Record<?> zeebeRecord, Logger logger) {
        String defaultKey = Long.toString(zeebeRecord.getKey());
        if (zeebeRecord.getValue() instanceof MessageRecordValue) {
            return defaultKey;
        }
        Long processInstanceKey = null;
        RecordValue value = zeebeRecord.getValue();
        try {
            Method method = value.getClass().getMethod("getProcessInstanceKey");
            processInstanceKey = (Long) method.invoke(value);
        } catch (Exception e) {
            logger.warn("Failed to get process instance key from record of type {}, proceeding with record key", value.getClass().getName());
        }
        return processInstanceKey != null && processInstanceKey > 0 ? Long.toString(processInstanceKey) : defaultKey;
    }

    private void send(ProducerRecord<String, String> message) {
        pending.add(new PendingRecord(message, producer.send(message)));
    }

    /**
     * Waits until Kafka has acknowledged every pending record. Records from the first
     * failed one onwards are sent again, in order, and checked on the next flush. A record
     * Kafka will never accept (too large) is dropped with an error instead of blocking export.
     *
     * @return true if Kafka acknowledged all pending records
     */
    public boolean flush() {
        if (pending.isEmpty()) {
            return true;
        }
        producer.flush();

        int firstFailed = firstFailedIndex();
        if (firstFailed < 0) {
            logger.info("flushed {} exported records to Kafka", pending.size());
            pending.clear();
            return true;
        }

        List<PendingRecord> unconfirmed = new ArrayList<>(pending.subList(firstFailed, pending.size()));
        pending.clear();
        unconfirmed.forEach(unsent -> send(unsent.message()));
        logger.error("Kafka did not acknowledge {} exported records, sending them again", unconfirmed.size());
        return false;
    }

    private int firstFailedIndex() {
        for (int i = 0; i < pending.size(); i++) {
            try {
                pending.get(i).result().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return i;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RecordTooLargeException) {
                    logger.error("Dropping record with key {}: larger than Kafka accepts", pending.get(i).message().key(), e.getCause());
                    continue;
                }
                logger.error("Failed to send record to Kafka", e.getCause());
                return i;
            }
        }
        return -1;
    }

    public boolean shouldFlush() {
        return pending.size() >= configuration.bulk.size;
    }

    private record PendingRecord(ProducerRecord<String, String> message, Future<RecordMetadata> result) {
    }

}
