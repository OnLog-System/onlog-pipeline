package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerFactory {

    public static KafkaProducer<String, String> create(String bootstrapServers) {

        Properties props = new Properties();

        // ==================================================
        // Basic
        // ==================================================
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // ==================================================
        // 🔥 Realtime 안정화 핵심 설정
        // ==================================================
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");        // 100ms 모이면 전송
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");     // 32KB
        props.put(ProducerConfig.RETRIES_CONFIG, "10");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");

        // (acks=all 이면 idempotence 자동 활성화)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // ==================================================
        // AWS MSK IAM
        // ==================================================
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put(
            "sasl.jaas.config",
            "software.amazon.msk.auth.iam.IAMLoginModule required;"
        );
        props.put(
            "sasl.client.callback.handler.class",
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        );

        return new KafkaProducer<>(props);
    }
}
