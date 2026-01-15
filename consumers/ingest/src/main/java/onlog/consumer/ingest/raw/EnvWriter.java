package onlog.consumer.ingest.raw;

import onlog.common.model.CanonicalEvent;
import onlog.consumer.ingest.DbConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class EnvWriter {

    private static final String SQL = """
        INSERT INTO raw.sensor_env (
          event_time,
          edge_ingest_time,
          tenant_id,
          line_id,
          process,
          device_type,
          metric,
          dev_eui,
          temperature,
          humidity,
          battery_mv,
          source_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

            ps.setObject(9, null);   // temperature (decoded elsewhere if needed)
            ps.setObject(10, null);  // humidity
            ps.setObject(11, null);  // battery
            ps.setString(12, e.sourceId);

            ps.executeUpdate();
        }
    }
}
