package onlog.streams.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import onlog.common.model.CanonicalEvent;
import onlog.common.serde.CanonicalEventSerde;
import onlog.common.serde.JsonSerde;
import onlog.common.time.TimeNormalizer;
import onlog.common.util.SourceIdUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class ParserTopology {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void build(StreamsBuilder builder) {

        // =========================
        // Dedup StateStore
        // =========================
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                DedupStoreSupplier.supplier(),
                Serdes.String(),
                Serdes.Long()
            )
        );

        KStream<String, String> env =
                builder.stream(ParserConfig.TOPIC_ENV,
                        Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> scale =
                builder.stream(ParserConfig.TOPIC_SCALE,
                        Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> machine =
                builder.stream(ParserConfig.TOPIC_MACHINE,
                        Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, ParsedWrapper> parsed =
                env.merge(scale)
                   .merge(machine)
                   .mapValues(ParserTopology::parseRaw);

        // =========================
        // Dedup
        // =========================
        KStream<String, ParsedWrapper> deduped =
            parsed.process(
                DedupTransformer::new,
                Named.as("dedup"),
                DedupStoreSupplier.STORE_NAME
            );
            
        // =========================
        // 정상 / 에러 분기
        // =========================
        KStream<String, ParsedWrapper>[] branches =
            deduped.branch(
                // DLQ: 구조적/계약 위반
                (k, v) ->
                    v == null
                    || v.meta != null && v.meta.containsKey("error")
                    || v.edgeIngestTime == null
                    || v.devEui == null
                    || v.metric == null,

                // 정상
                (k, v) -> true
            );

        // =========================
        // DLQ topic
        // =========================
        branches[0]
            .mapValues(v -> {
                ParseErrorEvent e = new ParseErrorEvent();
                e.occurredAt = Instant.now();

                if (v == null || v.meta == null) {
                    e.reason = "NULL_EVENT";
                    e.raw = null;
                } else {
                    e.reason = String.valueOf(v.meta.get("error"));
                    e.raw = String.valueOf(v.meta.get("raw"));
                }
                return e;
            })
            .to(
                "sensor.parse.dlq",
                Produced.with(Serdes.String(), new JsonSerde<>(ParseErrorEvent.class))
            );

        // =========================
        // 정상 Canonical flow
        // =========================
        branches[1]
            .mapValues(ParserTopology::toCanonical)
            .to(
                ParserConfig.OUTPUT_TOPIC,
                Produced.with(Serdes.String(), new CanonicalEventSerde())
            );
    }

    private static ParsedWrapper parseRaw(String raw) {

        ParsedWrapper w = new ParsedWrapper();

        try {
            JsonNode root = MAPPER.readTree(raw);

            // -------------------------
            // edge_ingest_time
            // -------------------------
            w.edgeIngestTime =
                TimeNormalizer.parseIso(
                    root.path("received_at").asText(null)
                );

            // -------------------------
            // Routing meta
            // -------------------------
            w.tenantId   = root.path("tenant_id").asText(null);
            w.lineId     = root.path("line_id").asText(null);
            w.process    = root.path("process").asText(null);
            w.deviceType = root.path("device_type").asText(null);
            w.metric     = root.path("metric").asText(null);

            // -------------------------
            // Payload
            // -------------------------
            JsonNode payload = MAPPER.readTree(
                root.path("payload").asText()
            );


            // -------------------------
            // event_time (source time)
            // -------------------------
            String eventTimeStr =
                payload.has("eventTime")
                    ? payload.path("eventTime").asText(null)
                    : payload.path("time").asText(null);

            w.eventTime = TimeNormalizer.parseIso(eventTimeStr);

            // -------------------------
            // Device info
            // -------------------------
            JsonNode deviceInfo = payload.path("deviceInfo");

            w.devEui      = deviceInfo.path("devEui").asText(null);
            w.deviceName  = deviceInfo.path("deviceName").asText(null);

            w.fCnt = payload.has("fCnt") ? payload.get("fCnt").asInt() : null;

            // =========================
            // Base64 payload decode
            // =========================
            if (payload.has("data")) {

                BatteryPayloadDecoder
                    .decode(payload.get("data").asText())
                    .ifPresent(d -> {

                        w.batteryMv     = d.batteryMv();
                        w.batteryStatus = d.batteryStatus();
                        w.temperature   = d.temperature();
                        w.humidity      = d.humidity();

                        // metric routing
                        if (w.metric != null) {
                            switch (w.metric) {
                                case "TEMP" ->
                                    w.valueNum = w.temperature;
                                case "HUMIDITY" ->
                                    w.valueNum = w.humidity;
                                case "BATTERY_MV" ->
                                    w.valueNum = (double) w.batteryMv;
                            }
                        }
                    });
            }

            return w;

        } catch (Exception e) {
            Map<String, Object> meta = new HashMap<>();
            meta.put("error", "PARSE_FAILED");
            meta.put("raw", raw);
            w.meta = meta;
            return w;
        }
    }

    private static CanonicalEvent toCanonical(ParsedWrapper w) {

        CanonicalEvent e = new CanonicalEvent();

        e.eventTime      = w.eventTime;
        e.edgeIngestTime = w.edgeIngestTime;

        e.tenantId = w.tenantId;
        e.lineId   = w.lineId;
        e.process  = w.process;

        e.devEui     = w.devEui;
        e.deviceType = w.deviceType;
        e.metric     = w.metric;
        e.deviceName = w.deviceName;

        e.valueNum  = w.valueNum;
        e.valueBool = w.valueBool;

        e.fCnt = w.fCnt;

        e.batteryMv     = w.batteryMv;
        e.batteryStatus = w.batteryStatus;
        e.temperature   = w.temperature;
        e.humidity      = w.humidity;

        e.sourceId = SourceIdUtil.build(
                w.tenantId,
                w.lineId,
                w.process,
                w.deviceType,
                w.metric
        );

        return e;
    }
}
