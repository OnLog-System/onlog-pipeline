package onlog.common.model;

import java.time.Instant;
import java.util.Map;

/**
 * Canonical normalized event
 * Produced by streams-parser
 * Consumed by streams-kpi / ingest-consumer
 */
public class CanonicalEvent {

    // =========================
    // Time (single source of truth)
    // =========================
    public Instant edgeIngestTime;

    // =========================
    // Identity
    // =========================
    public String tenantId;
    public String lineId;
    public String process;

    public String devEui;
    public String deviceType;
    public String metric;

    // =========================
    // Value
    // =========================
    public Double valueNum;
    public Boolean valueBool;

    // =========================
    // Dedup
    // =========================
    public Integer fCnt;

    // =========================
    // Source
    // =========================
    public String sourceId;

    // =========================
    // Optional meta 둘지 말지 결정하자!
    // =========================
    // public Map<String, Object> meta;
}
