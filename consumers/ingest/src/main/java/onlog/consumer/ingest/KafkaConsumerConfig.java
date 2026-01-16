package onlog.consumer.ingest;

import onlog.common.serde.CanonicalEventSerde;
import onlog.common.serde.JsonSerde;
import onlog.common.model.KpiEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerConfig {

    public static Properties base(String groupId) {

        Properties p = new Properties();

        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv("KAFKA_BOOTSTRAP"));

        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);

        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);

        return p;
    }
}
