package onlog.streams.kpi;

public class KpiEvent {

    public long snapshotTime;

    public String tenantId;
    public String lineId;

    public String kpiType;
    public String kpiKey;

    public Double valueNum;
    public String valueText;
    public Boolean valueBool;

    public static KpiEvent production(long ts, String key, double total) {
        String[] parts = key.split("\\|");

        KpiEvent e = new KpiEvent();
        e.snapshotTime = ts;
        e.tenantId = parts[0];
        e.lineId   = parts[1];

        e.kpiType = "production";
        e.kpiKey  = "total_weight";
        e.valueNum = total;
        return e;
    }

    public static KpiEvent yield(long ts, String key, double ratio) {
        String[] parts = key.split("\\|");

        KpiEvent e = new KpiEvent();
        e.snapshotTime = ts;
        e.tenantId = parts[0];
        e.lineId   = parts[1];

        e.kpiType = "yield";
        e.kpiKey  = "yield_ratio";
        e.valueNum = ratio;
        return e;
    }
}
