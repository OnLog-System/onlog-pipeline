package onlog.streams.kpi;

import onlog.common.model.CanonicalEvent;
import onlog.common.serde.CanonicalEventSerde;
import onlog.common.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

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
            .mapValues((w, total) ->
                    KpiEvent.production(
                            w.window().endTime().toEpochMilli(),
                            w.key(),
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
                    (key, event, agg) -> YieldAggregator.add(agg, event),
                    YieldAggregator::add,
                    Materialized.with(
                            Serdes.String(),
                            new JsonSerde<>(YieldAggregator.YieldCount.class)
                    )
            )
            .toStream()
            .mapValues((w, yc) ->
                    KpiEvent.yield(
                            w.window().endTime().toEpochMilli(),
                            w.key(),
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
