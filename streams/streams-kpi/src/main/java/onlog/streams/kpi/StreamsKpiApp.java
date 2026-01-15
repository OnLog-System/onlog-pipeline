package onlog.streams.kpi;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamsKpiApp {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                KpiConfig.APPLICATION_ID
        );

        props.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv("KAFKA_BOOTSTRAP")
        );

        props.put(
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                StreamsConfig.EXACTLY_ONCE_V2
        );

        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.Serdes$StringSerde"
        );

        props.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.Serdes$ByteArraySerde"
        );

        StreamsBuilder builder = new StreamsBuilder();
        KpiTopology.build(builder);

        KafkaStreams streams =
                new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(
                new Thread(streams::close)
        );

        streams.start();
    }
}
