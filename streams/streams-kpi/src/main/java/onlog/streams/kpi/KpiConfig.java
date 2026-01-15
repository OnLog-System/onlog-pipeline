package onlog.streams.kpi;

import java.time.Duration;

public class KpiConfig {

    public static final String APPLICATION_ID = "streams-kpi";

    public static final String INPUT_TOPIC  = "sensor.parsed";
    public static final String OUTPUT_TOPIC = "kpi.event";

    // device types
    public static final String DEVICE_PACK_SCALE = "PACK_SCALE";
    public static final String DEVICE_UNIT_SCALE = "UNIT_SCALE";

    // metric
    public static final String METRIC_WEIGHT = "WEIGHT";

    // yield threshold (g)
    public static final double YIELD_MIN = 13.0;
    public static final double YIELD_MAX = 15.0;

    // window
    public static final Duration WINDOW_SIZE = Duration.ofDays(1);
    public static final Duration GRACE       = Duration.ofMinutes(2);

    private KpiConfig() {}
}
