package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

// Group number:
// Group members:

// How many partitions can the topics "temperature" and "humidity" have?

// Is there any relation between the number of partitions in "temperature" and "humidity"?

// Is it possible to have more than one instance of Monitor?
// If so, what is the relation between their group id?

// How many partitions can the topic "alerts" have?

// Is it possible to have more than one instance of Reactor?
// If so, what is the relation between their group id?

// How many partitions can the topic "processedAlerts" have?

// Which additional topics, if any, do you use? How many partitions can they have?

public class ConsumersXX {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];
        // TODO: add arguments if necessary
        switch (stage) {
            case 1:
                new Monitor(serverAddr, groupId).execute();
                break;
            case 2:
                new Reactor(serverAddr, groupId).execute();
                break;
            default:
                System.err.println("Wrong stage");
        }
    }

    // Simulates the first reaction of the system to an alert (provided, do not
    // modify)
    private static void reaction1(String room, String alert) {
        System.out.println("[reaction1] room=" + room + " " + alert);
    }

    // Simulates the second reaction of the system to an alert (provided, do not
    // modify)
    private static void reaction2(String room, String alert) {
        System.out.println("[reaction2] room=" + room + " " + alert);
    }

    private static class Monitor {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String temperaturesTopic = "temperature";
        private static final String humiditiesTopic = "humidity";
        private static final String outputTopic = "alerts";

        private static final int windowSize = 3;
        private static final double temperatureThreshold = 28;
        private static final double humidityThreshold = 40;

        // TODO: add attributes if necessary

        // TODO: add arguments if necessary
        public Monitor(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Arrays.asList(temperaturesTopic, humiditiesTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            final List<ConsumerRecord<String, Integer>> TemperatureWindow = new ArrayList<>(windowSize);
            final List<ConsumerRecord<String, Integer>> HumidityWindow = new ArrayList<>(windowSize);

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    final String topic = record.topic();
                    final String key = record.key();
                    final Integer value = record.value();
                    System.out.println("Received key: " + key + " value: " + value + " from topic: " + topic);
                    if (topic.equals(temperaturesTopic)) {
                        TemperatureWindow.add(record);
                        if (TemperatureWindow.size() >= windowSize) {
                            System.out.println("Temperatures are full");
                            int[] tempValues = new int[windowSize];
                            for (int i = 0; i < TemperatureWindow.size(); i++) {
                                tempValues[i] = TemperatureWindow.get(i).value();
                            }
                            double avgTemp = Arrays.stream(tempValues).average().orElse(0.0);
                            if (avgTemp > temperatureThreshold) {
                                System.out.println("Temperature threshold exceeded: " + avgTemp);
                                // Send alert to output topic
                                producer.send(new ProducerRecord<>(outputTopic, key, "Temperature alert: " + avgTemp));
                            }

                            TemperatureWindow.clear();
                            producer.flush();
                            consumer.commitSync();
                        } else if (topic.equals(humiditiesTopic)) {
                            HumidityWindow.add(record);
                            if (HumidityWindow.size() >= windowSize) {
                                System.out.println("Humidities are full");
                                HumidityWindow.add(record);
                                int[] humValues = new int[windowSize];
                                for (int i = 0; i < HumidityWindow.size(); i++) {
                                    humValues[i] = HumidityWindow.get(i).value();
                                }
                                double avgHum = Arrays.stream(humValues).average().orElse(0.0);
                                if (avgHum < humidityThreshold) {
                                    System.out.println("Humidity threshold exceeded: " + avgHum);
                                    // Send alert to output topic
                                    producer.send(new ProducerRecord<>(outputTopic, key, "Humidity alert: " + avgHum));
                                }

                                HumidityWindow.clear();
                                producer.flush();
                                consumer.commitSync();
                            }
                        }
                    }
                }
            }
        }
    }

        private static class Reactor {
            private final String serverAddr;
            private final String consumerGroupId;

            private static final String inputTopic = "alerts";
            private static final String outputTopic = "processedAlerts";

            // TODO: add attributes if needed

            // TODO: add arguments if necessary
            public Reactor(String serverAddr, String consumerGroupId) {
                this.serverAddr = serverAddr;
                this.consumerGroupId = consumerGroupId;
            }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            // disable the auto commit of offset
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
            // subscribe to "Alerts" and "ProcessAlert"
            consumer.subscribe(Arrays.asList(inputTopic, outputTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // TODO: add properties if needed

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            

            while (true) {
                consumer.poll(Duration.of(5, ChronoUnit.MINUTES))

                for (ConsumerRecord<String, String> record : records) {
                    final String topic = record.topic();
                    final String key = record.key();
                    final String value = record.value();

                    if (topic.equals(inputTopic)) {

                    } else if (topic.equals(outputTopic)) {

                    }
            }
            
            // repeat at least ones
            consumer.commitSync();
        }
    }
}
