package onlog.streams.kpi;

import onlog.common.model.CanonicalEvent;
import onlog.common.serde.CanonicalEventSerde;
import onlog.common.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

public class KpiTopology {

    public static void build(StreamsBuilder builder) {

        KStream<String, CanonicalEvent> stream =
                builder.stream(
                        KpiConfig.INPUT_TOPIC,
                        Consumed.with(
                                Serdes.String(),
                                new CanonicalEventSerde()
                        )
                );

        // key = tenant|line
        KStream<String, CanonicalEvent> keyed =
                stream.selectKey((k, v) ->
                        v.tenantId + "|" + v.lineId
                );

        TimeWindows window =
                TimeWindows.ofSizeAndGrace(
                        KpiConfig.WINDOW_SIZE,
                        KpiConfig.GRACE
                );

        // =========================
        // 1. Production (PACK_SCALE)
        // =========================
        keyed
            .filter((k, v) ->
                    KpiConfig.DEVICE_PACK_SCALE.equals(v.deviceType) &&
                    KpiConfig.METRIC_WEIGHT.equals(v.metric)
            )
            .groupByKey()
            .windowedBy(window)
            .aggregate(
                    () -> 0.0,
                    (key, event, agg) -> ProductionAggregator.add(agg, event),
                    Materialized.with(
                            Serdes.String(),
                            Serdes.Double()
                    )
            )
            .toStream()
            // Windowed<String> → String
            .selectKey((windowedKey, v) -> windowedKey.key())
            .mapValues((k, total) ->
                    KpiEvent.production(
                            System.currentTimeMillis(),
                            k,
                            total
                    )
            )
            .to(
                KpiConfig.OUTPUT_TOPIC,
                Produced.with(
                        Serdes.String(),
                        new JsonSerde<>(KpiEvent.class)
                )
            );

        // =========================
        // 2. Yield (UNIT_SCALE)
        // =========================
        keyed
            .filter((k, v) ->
                    KpiConfig.DEVICE_UNIT_SCALE.equals(v.deviceType) &&
                    KpiConfig.METRIC_WEIGHT.equals(v.metric)
            )
            .groupByKey()
            .windowedBy(window)
            .aggregate(
                    YieldAggregator.YieldCount::new,
                    (key, event, agg) -> YieldAggregator.add(agg, event),
                    Materialized.with(
                            Serdes.String(),
                            new JsonSerde<>(YieldAggregator.YieldCount.class)
                    )
            )
            .toStream()
            // Windowed<String> → String
            .selectKey((windowedKey, v) -> windowedKey.key())
            .mapValues((k, yc) ->
                    KpiEvent.yield(
                            System.currentTimeMillis(),
                            k,
                            yc.ratio()
                    )
            )
            .to(
                KpiConfig.OUTPUT_TOPIC,
                Produced.with(
                        Serdes.String(),
                        new JsonSerde<>(KpiEvent.class)
                )
            );
    }
}
