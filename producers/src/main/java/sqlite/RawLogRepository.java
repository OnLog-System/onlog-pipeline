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

    // payload 내 devEui 추출 (파싱 ❌, 문자열 처리만)
    private static final Pattern DEV_EUI_PATTERN =
            Pattern.compile("\"devEui\"\\s*:\\s*\"([^\"]+)\"");

    public RawLogRepository(Connection conn) {
        this.conn = conn;
    }

    public List<RawLogRow> findBySlot(Instant slot) throws Exception {

        String sql = """
            SELECT id, received_at, topic, tenant_id, line_id,
                   process, device_type, metric, payload
            FROM raw_logs
            WHERE received_at = ?
            ORDER BY tenant_id, line_id, device_type, metric
        """;

        List<RawLogRow> rows = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, slot.toString());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
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

                    rows.add(r);
                }
            }
        }
        return rows;
    }

    public List<RawLogRow> findAllOrdered() throws Exception {

        String sql = """
            SELECT id, received_at, topic, tenant_id, line_id,
                process, device_type, metric, payload
            FROM raw_logs
            ORDER BY received_at ASC
        """;

        List<RawLogRow> rows = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
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

                rows.add(r);
            }
        }
        return rows;
    }


    private String extractDevEui(String payload) {
        if (payload == null) return null;
        Matcher m = DEV_EUI_PATTERN.matcher(payload);
        return m.find() ? m.group(1) : null;
    }
}
