package onlog.streams.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import onlog.common.model.CanonicalEvent;
import onlog.common.serde.CanonicalEventSerde;
import onlog.common.time.TimeNormalizer;
import onlog.common.util.SourceIdUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class ParserTopology {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void build(StreamsBuilder builder) {
        
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

        KStream<String, ParsedWrapper> deduped =
            parsed.process(
                DedupTransformer::new,
                Named.as("dedup"),
                DedupStoreSupplier.STORE_NAME
            );

        deduped
            .mapValues(ParserTopology::toCanonical)
            .to(
                ParserConfig.OUTPUT_TOPIC,
                Produced.with(Serdes.String(), new CanonicalEventSerde())
            );
    }

    private static ParsedWrapper parseRaw(String raw) {
        try {
            JsonNode root = MAPPER.readTree(raw);
            ParsedWrapper w = new ParsedWrapper();

            // -------------------------
            // edge_ingest_time (single truth)
            // -------------------------
            w.edgeIngestTime =
                    TimeNormalizer.parseIso(root.get("received_at").asText());

            // -------------------------
            // Routing meta
            // -------------------------
            w.tenantId = root.path("tenant_id").asText();
            w.lineId   = root.path("line_id").asText(null);
            w.process  = root.path("process").asText(null);
            w.deviceType = root.path("device_type").asText();
            w.metric     = root.path("metric").asText();

            // -------------------------
            // Payload
            // -------------------------
            JsonNode payload = MAPPER.readTree(root.get("payload").asText());

            w.devEui = payload.path("deviceInfo").path("devEui").asText(null);
            w.fCnt   = payload.has("fCnt") ? payload.get("fCnt").asInt() : null;

            // -------------------------
            // Value normalization
            // -------------------------
            if (payload.has("value")) {
                w.valueNum = payload.get("value").asDouble();
            }
            if (payload.has("value_bool")) {
                w.valueBool = payload.get("value_bool").asBoolean();
            }
            if (payload.has("values") && payload.get("values").has("weight")) {
                w.valueNum = payload.get("values").get("weight").asDouble();
            }

            // -------------------------
            // Meta (reference only)
            // -------------------------
            Map<String, Object> meta = new HashMap<>();
            meta.put("payload", payload);
            w.meta = meta;

            return w;

        } catch (Exception e) {
            return null;
        }
    }

    private static CanonicalEvent toCanonical(ParsedWrapper w) {

        CanonicalEvent e = new CanonicalEvent();

        e.edgeIngestTime = w.edgeIngestTime;

        e.tenantId = w.tenantId;
        e.lineId   = w.lineId;
        e.process  = w.process;

        e.devEui     = w.devEui;
        e.deviceType = w.deviceType;
        e.metric     = w.metric;

        e.valueNum  = w.valueNum;
        e.valueBool = w.valueBool;

        e.fCnt = w.fCnt;

        e.sourceId = SourceIdUtil.build(
                w.tenantId,
                w.lineId,
                w.process,
                w.deviceType,
                w.metric
        );

        e.meta = w.meta;
        return e;
    }
}
