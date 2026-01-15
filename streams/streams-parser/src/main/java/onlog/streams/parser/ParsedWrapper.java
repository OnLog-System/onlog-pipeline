package onlog.streams.parser;

import java.time.Instant;
import java.util.Map;

/**
 * Internal intermediate structure
 * Used only inside streams-parser
 */
public class ParsedWrapper {

    public Instant edgeIngestTime;

    public String tenantId;
    public String lineId;
    public String process;

    public String devEui;
    public String deviceType;
    public String metric;

    public Double valueNum;
    public Boolean valueBool;

    public Integer fCnt;

    public Map<String, Object> meta;
}
