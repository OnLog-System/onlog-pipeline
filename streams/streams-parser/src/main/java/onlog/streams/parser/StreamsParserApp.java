package onlog.streams.parser;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamsParserApp {

    public static void main(String[] args) {

        Properties props = new Properties();

        // =========================
        // Core
        // =========================
        props.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            ParserConfig.APPLICATION_ID
        );

        props.put(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            System.getenv("KAFKA_BOOTSTRAP")
        );

        // =========================
        // MSK IAM (🔥 필수 🔥)
        // =========================
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

        // =========================
        // Serde / Time
        // =========================
        props.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.Serdes$StringSerde"
        );

        props.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.Serdes$StringSerde"
        );

        props.put(
            StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            EdgeIngestTimeExtractor.class
        );

        // =========================
        // Build & Start
        // =========================
        StreamsBuilder builder = new StreamsBuilder();
        ParserTopology.build(builder);

        KafkaStreams streams =
            new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(
            new Thread(streams::close)
        );

        streams.start();
    }
}
