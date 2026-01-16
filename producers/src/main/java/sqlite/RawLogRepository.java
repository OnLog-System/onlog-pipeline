package sqlite;

import model.RawLogRow;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RawLogRepository {

    private final Connection conn;

    private static final Pattern DEV_EUI_PATTERN =
            Pattern.compile("\"devEui\"\\s*:\\s*\"([^\"]+)\"");

    public RawLogRepository(Connection conn) {
        this.conn = conn;
    }

    /**
     * Realtime / Backfill 공용
     * (from, to] + LIMIT
     */
    public List<RawLogRow> findBetween(Instant from, Instant to, int limit) throws Exception {

        String sql = """
            SELECT id, received_at, topic, tenant_id, line_id,
                   process, device_type, metric, payload
            FROM raw_logs
            WHERE received_at > ?
              AND received_at <= ?
            ORDER BY received_at ASC
            LIMIT ?
        """;

        List<RawLogRow> rows = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, from.toString());
            ps.setString(2, to.toString());
            ps.setInt(3, limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(mapRow(rs));
                }
            }
        }
        return rows;
    }

    /**
     * Realtime용 (window)
     */
    public List<RawLogRow> findBetween(Instant from, Instant to) throws Exception {
        return findBetween(from, to, Integer.MAX_VALUE);
    }

    private RawLogRow mapRow(ResultSet rs) throws Exception {

        RawLogRow r = new RawLogRow();

        r.id = rs.getLong("id");
        r.receivedAt = Instant.parse(rs.getString("received_at"));
        r.topic = rs.getString("topic");

        r.tenantId = rs.getString("tenant_id");
        r.lineId = rs.getString("line_id");
        r.process = rs.getString("process");
        r.deviceType = rs.getString("device_type");
        r.metric = rs.getString("metric");

        r.payload = rs.getString("payload");
        r.devEui = extractDevEui(r.payload);

        return r;
    }

    private String extractDevEui(String payload) {
        if (payload == null) return null;
        Matcher m = DEV_EUI_PATTERN.matcher(payload);
        return m.find() ? m.group(1) : null;
    }
}
