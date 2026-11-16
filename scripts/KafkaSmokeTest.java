import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

// Verifies the existing Producer with real Kafka, without running the unfinished consumers.
class KafkaSmokeTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        Process producer = null;
        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (String topic : List.of("temperature", "humidity")) {
                consumer.partitionsFor(topic).forEach(p -> partitions.add(new TopicPartition(topic, p.partition())));
            }
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            for (TopicPartition partition : partitions) consumer.position(partition);
            producer = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("java.class.path"), "it.polimi.nsds.kafka.eval.Producer")
                    .redirectErrorStream(true)
                    .redirectOutput(Path.of(".local/logs/smoke-producer.log").toFile())
                    .start();
            Set<String> seen = new HashSet<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (seen.size() < 2 && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofSeconds(1))) {
                    if (record.key() == null || !record.key().matches("Key[0-9]")
                            || record.value() == null || record.value() < 0 || record.value() >= 100) {
                        throw new AssertionError("Messaggio non valido: " + record);
                    }
                    seen.add(record.topic());
                    System.out.printf("Ricevuto: %s partition=%d key=%s value=%d%n",
                            record.topic(), record.partition(), record.key(), record.value());
                }
                if (!producer.isAlive()) throw new AssertionError("Producer terminato: vedere .local/logs/smoke-producer.log");
            }
            if (seen.size() != 2) throw new AssertionError("Messaggi mancanti entro 20 secondi; topic ricevuti: " + seen);
            System.out.println("OK: il Producer del progetto invia correttamente su temperature e humidity.");
        } finally {
            if (producer != null) {
                producer.destroy();
                if (!producer.waitFor(5, TimeUnit.SECONDS)) producer.destroyForcibly();
            }
        }
    }
}
