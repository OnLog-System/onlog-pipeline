package onlog.consumer.ingest;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class IngestApp {

    public static void main(String[] args) throws Exception {

        Properties props =
                KafkaConsumerConfig.base("ingest-consumer");

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(props)) {

            consumer.subscribe(
                    List.of("sensor.parsed", "kpi.event")
            );

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));

                records.forEach(r -> {
                    try {
                        Dispatcher.dispatch(r.topic(), r.value());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                consumer.commitSync();
            }
        }
    }
}
