package onlog.streams.parser;

import java.time.Duration;

public class ParserConfig {

    public static final String APPLICATION_ID = "streams-parser";

    public static final String TOPIC_ENV     = "sensor.env.raw";
    public static final String TOPIC_SCALE   = "sensor.scale.raw";
    public static final String TOPIC_MACHINE = "machine.raw";

    public static final String OUTPUT_TOPIC  = "sensor.parsed";

    // Dedup TTL (wall-clock, edge_ingest_time 기준)
    public static final Duration DEDUP_TTL = Duration.ofMinutes(30);

    private ParserConfig() {}
}
