package onlog.consumer.ingest.kpi;

import onlog.consumer.ingest.DbConnectionPool;
import onlog.common.model.KpiEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class KpiWriter {

    private static final String SQL = """
        INSERT INTO derived.kpi (
          snapshot_time,
          tenant_id,
          line_id,
          kpi_type,
          kpi_key,
          value_num
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """;

    public static void write(KpiEvent e) throws Exception {

        try (Connection c = DbConnectionPool.get();
             PreparedStatement ps = c.prepareStatement(SQL)) {

            ps.setObject(1, e.snapshotTime);
            ps.setString(2, e.tenantId);
            ps.setString(3, e.lineId);
            ps.setString(4, e.kpiType);
            ps.setString(5, e.kpiKey);
            ps.setDouble(6, e.valueNum);

            ps.executeUpdate();
        }
    }
}
