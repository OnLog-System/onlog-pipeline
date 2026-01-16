package onlog.streams.parser;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

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

        // ⚠️ state.dir 명시 (lock / 충돌 방지)
        props.put(
            StreamsConfig.STATE_DIR_CONFIG,
            "/tmp/kafka-streams-parser"
        );

        // =========================
        // MSK IAM (필수)
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
        // Build topology
        // =========================
        StreamsBuilder builder = new StreamsBuilder();
        ParserTopology.build(builder);

        // Build topology once
        Topology topology = builder.build();

        // 🔥 토폴로지 출력
        System.out.println("========== STREAMS TOPOLOGY ==========");
        System.out.println(topology.describe());
        System.out.println("======================================");

        // Create streams
        KafkaStreams streams = new KafkaStreams(topology, props);

        // =========================
        // 🔥 상태 추적 (핵심)
        // =========================
        streams.setStateListener((newState, oldState) -> {
            System.out.println(
                "### STREAMS STATE: " + oldState + " -> " + newState
            );
        });

        // =========================
        // 🔥 예외 추적 (핵심)
        // =========================
        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            System.err.println(
                "### UNCAUGHT EXCEPTION in thread: " + thread.getName()
            );
            throwable.printStackTrace();
        });

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                System.out.println("### SHUTDOWN");
                streams.close();
            })
        );

        System.out.println("### STARTING STREAMS");
        streams.start();
    }
}
