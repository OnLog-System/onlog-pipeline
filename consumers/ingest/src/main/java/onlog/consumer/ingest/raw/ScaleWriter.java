package onlog.consumer.ingest.raw;

import onlog.common.model.CanonicalEvent;
import onlog.consumer.ingest.DbConnectionPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;

public class ScaleWriter {

    private static final DataSource ds =
            DbConnectionPool.dataSource();

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

        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(SQL)) {

            ps.setObject(1, e.eventTime);
            ps.setObject(2, e.edgeIngestTime);

            ps.setString(3, e.tenantId);
            ps.setString(4, e.lineId);
            ps.setString(5, e.process);
            ps.setString(6, e.deviceType);
            ps.setString(7, e.metric);
            ps.setString(8, e.deviceName);
            ps.setString(9, e.devEui);

            if (e.valueNum != null) {
                ps.setDouble(10, e.valueNum);
            } else {
                ps.setNull(10, Types.DOUBLE);
            }

            ps.setString(11, e.sourceId);

            ps.executeUpdate();
        }
    }
}
