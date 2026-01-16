package onlog.streams.parser;

import java.time.Instant;
import java.util.Map;

/**
 * Internal intermediate structure
 * Used only inside streams-parser
 */
public class ParsedWrapper {

    public Instant eventTime;
    public Instant edgeIngestTime;

    public String tenantId;
    public String lineId;
    public String process;

    public String devEui;
    public String deviceType;
    public String metric;
    public String deviceName;

    // =========================
    // Decoded values
    // =========================
    public Double valueNum;
    public Boolean valueBool;

    public Integer batteryMv;
    public String batteryStatus;

    public Double temperature;
    public Double humidity;

    public Integer fCnt;

    public Map<String, Object> meta;
}
