package onlog.streams.kpi;

import onlog.common.model.CanonicalEvent;

public class ProductionAggregator {

    public static Double add(Double agg, CanonicalEvent e) {
        if (agg == null) {
            agg = 0.0;
        }
        if (e.valueNum != null) {
            agg += e.valueNum;
        }
        return agg;
    }
}
