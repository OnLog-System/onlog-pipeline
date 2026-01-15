package onlog.streams.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class EdgeIngestTimeExtractor implements TimestampExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {

        Object value = record.value();

        if (value instanceof String) {
            try {
                JsonNode root = MAPPER.readTree((String) value);
                String ts = root.path("received_at").asText(null);
                if (ts != null) {
                    return Instant.parse(ts).toEpochMilli();
                }
            } catch (Exception ignored) {}
        }

        return record.timestamp(); // fallback
    }
}
