package onlog.consumer.ingest.raw;

import onlog.common.model.CanonicalEvent;
import onlog.consumer.ingest.DbConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class ScaleWriter {

    private static final String SQL = """
        INSERT INTO raw.sensor_scale (
          event_time,
          edge_ingest_time,
          tenant_id,
          line_id,
          process,
          device_type,
          metric,
          dev_eui,
          weight,
          source_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

    public static void write(CanonicalEvent e) throws Exception {

        try (Connection c = DbConnectionPool.get();
             PreparedStatement ps = c.prepareStatement(SQL)) {

            ps.setObject(1, e.edgeIngestTime);
            ps.setObject(2, e.edgeIngestTime);
            ps.setString(3, e.tenantId);
            ps.setString(4, e.lineId);
            ps.setString(5, e.process);
            ps.setString(6, e.deviceType);
            ps.setString(7, e.metric);
            ps.setString(8, e.devEui);
            ps.setDouble(9, e.valueNum);
            ps.setString(10, e.sourceId);

            ps.executeUpdate();
        }
    }
}
