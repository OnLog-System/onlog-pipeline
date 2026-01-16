package onlog.consumer.ingest.raw;

import onlog.common.model.CanonicalEvent;
import onlog.consumer.ingest.DbConnectionPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class EnvWriter {

    private static final DataSource ds =
            DbConnectionPool.dataSource();

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
          device_name,
          temperature,
          humidity,
          battery_mv,
          battery_status,
          source_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

    public static void write(CanonicalEvent e) throws Exception {

        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(SQL)) {

            // =========================
            // Time
            // =========================
            ps.setObject(1, e.eventTime);
            ps.setObject(2, e.edgeIngestTime);

            // =========================
            // Identity
            // =========================
            ps.setString(3, e.tenantId);
            ps.setString(4, e.lineId);
            ps.setString(5, e.process);
            ps.setString(6, e.deviceType);
            ps.setString(7, e.metric);

            ps.setString(8,  e.devEui);
            ps.setString(9,  e.deviceName);

            // =========================
            // Values
            // =========================
            ps.setObject(10, e.temperature);
            ps.setObject(11, e.humidity);
            ps.setObject(12, e.batteryMv);
            ps.setString(13, e.batteryStatus);

            // =========================
            // Source
            // =========================
            ps.setString(14, e.sourceId);

            ps.executeUpdate();
        }
    }
}
