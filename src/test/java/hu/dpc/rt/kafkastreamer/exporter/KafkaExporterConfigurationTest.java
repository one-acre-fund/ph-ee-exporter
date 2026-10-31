package hu.dpc.rt.kafkastreamer.exporter;

import io.camunda.zeebe.broker.exporter.context.ExporterConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Args are bound by Zeebe's own ExporterConfiguration, with the lower-case keys Spring produces
// from ZEEBE_BROKER_EXPORTERS_KAFKA_ARGS_* env vars.
class KafkaExporterConfigurationTest {

    @Test
    void bindsExporterArgsTheWayTheBrokerDoes() {
        KafkaExporterConfiguration configuration = fromArgs(Map.of(
                "kafkaurl", "kafka:9092",
                "kafkatopic", "zeebe-export-test",
                "kafkatopicpartitions", "6",
                "kafkatopicreplicationfactor", "3",
                "requesttimeoutms", "1000",
                "deliverytimeoutms", "5000",
                "bulk", Map.of("size", "10", "delay", "2")));

        assertEquals("kafka:9092", configuration.kafkaUrl);
        assertEquals("zeebe-export-test", configuration.kafkaTopic);
        assertEquals(6, configuration.kafkaTopicPartitions);
        assertEquals(3, configuration.kafkaTopicReplicationFactor);
        assertEquals(1000, configuration.requestTimeoutMs);
        assertEquals(5000, configuration.deliveryTimeoutMs);
        assertEquals(10, configuration.bulk.size);
        assertEquals(2, configuration.bulk.delay);
    }

    @Test
    void unsetArgsKeepTheDefaults() {
        KafkaExporterConfiguration configuration = fromArgs(Map.of("kafkaUrl", "kafka:9092"));

        assertEquals("zeebe-export", configuration.kafkaTopic);
        assertNull(configuration.kafkaTopicPartitions);
        assertNull(configuration.deliveryTimeoutMs);
        assertEquals(1_000, configuration.bulk.size);
        assertEquals(5, configuration.bulk.delay);
    }

    @Test
    void reportsEveryInvalidSettingAtOnce() {
        KafkaExporterConfiguration configuration = fromArgs(Map.of(
                "kafkaUrl", "kafka:9092",
                "kafkaTopicPartitions", "0",
                "requestTimeoutMs", "10000",
                "deliveryTimeoutMs", "5000"));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, configuration::validate);
        assertTrue(error.getMessage().contains("kafkaTopicPartitions must be at least 1"), error.getMessage());
        assertTrue(error.getMessage().contains("deliveryTimeoutMs (5000) must be at least requestTimeoutMs (10000)"), error.getMessage());
    }

    @Test
    void deliveryTimeoutIsCheckedAgainstKafkasDefaultRequestTimeout() {
        KafkaExporterConfiguration configuration = fromArgs(Map.of("kafkaUrl", "kafka:9092", "deliveryTimeoutMs", "5000"));

        assertThrows(IllegalArgumentException.class, configuration::validate);
    }

    @Test
    void blankOrMissingUrlAndTopicAreInvalid() {
        Map<String, Object> args = new HashMap<>();
        args.put("kafkaUrl", " ");
        args.put("kafkaTopic", null);
        KafkaExporterConfiguration configuration = fromArgs(args);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, configuration::validate);
        assertTrue(error.getMessage().contains("kafkaUrl is required"), error.getMessage());
        assertTrue(error.getMessage().contains("kafkaTopic must not be empty"), error.getMessage());

        configuration.kafkaTopic = " ";
        error = assertThrows(IllegalArgumentException.class, configuration::validate);
        assertTrue(error.getMessage().contains("kafkaTopic must not be empty"), error.getMessage());
    }

    @Test
    void aDeliveryTimeoutAboveTheRequestTimeoutIsValid() {
        KafkaExporterConfiguration configuration = fromArgs(Map.of("kafkaUrl", "kafka:9092", "requestTimeoutMs", "1000", "deliveryTimeoutMs", "5000"));

        assertDoesNotThrow(configuration::validate);
    }

    @Test
    void logsEverySetting() {
        String logged = fromArgs(Map.of("kafkaUrl", "kafka:9092", "deliveryTimeoutMs", "60000")).toString();

        assertTrue(logged.contains("kafkaUrl='kafka:9092'"), logged);
        assertTrue(logged.contains("deliveryTimeoutMs=60000"), logged);
        assertTrue(logged.contains("bulk=BulkConfiguration{delay=5, size=1000}"), logged);
    }

    @Test
    void envNumbersMustBeWholeNumbersWhenSet() {
        assertNull(KafkaExporterConfiguration.parseInt("ZEEBE_KAFKAEXPORT_TOPIC_PARTITIONS", null));
        assertEquals(6, KafkaExporterConfiguration.parseInt("ZEEBE_KAFKAEXPORT_TOPIC_PARTITIONS", " 6 "));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> KafkaExporterConfiguration.parseInt("ZEEBE_KAFKAEXPORT_TOPIC_PARTITIONS", "six"));
        assertEquals("ZEEBE_KAFKAEXPORT_TOPIC_PARTITIONS must be a whole number, got 'six'", error.getMessage());
    }

    private static KafkaExporterConfiguration fromArgs(Map<String, Object> args) {
        return new ExporterConfiguration("kafka", args).instantiate(KafkaExporterConfiguration.class);
    }
}
