package onlog.streams.kpi;

import onlog.common.model.CanonicalEvent;

public class YieldAggregator {

    public static YieldCount add(YieldCount agg, CanonicalEvent e) {
        if (agg == null) {
            agg = new YieldCount();
        }

        if (e.valueNum == null) {
            return agg;
        }

        double w = e.valueNum;
        if (w >= KpiConfig.YIELD_MIN && w <= KpiConfig.YIELD_MAX) {
            agg.ok++;
        } else {
            agg.no++;
        }
        return agg;
    }

    public static class YieldCount {
        public long ok = 0;
        public long no = 0;

        public double ratio() {
            long total = ok + no;
            return total == 0 ? 0.0 : (double) ok / total;
        }
    }
}
