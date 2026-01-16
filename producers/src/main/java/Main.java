import kafka.KafkaProducerFactory;
import kafka.KafkaSender;
import model.RawLogRow;
import sqlite.RawLogRepository;
import sqlite.SqliteClient;

import java.io.File;
import java.sql.Connection;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {

        String bootstrap = getenv("KAFKA_BOOTSTRAP_SERVERS");
        String basePath  = getenv("DB_BASE_PATH");
        String mode      = getenvOrDefault("PRODUCER_MODE", "realtime");

        var producer = KafkaProducerFactory.create(bootstrap);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                producer.flush();
            } finally {
                producer.close();
            }
        }));

        var sender = new KafkaSender(producer);

        System.out.println("[Producer mode] " + mode);

        if ("backfill".equals(mode)) {
            runBackfill(basePath, sender);
            sender.flush();
            System.out.println("[Backfill completed]");
            return;
        }

        runRealtime(basePath, sender);
    }

    // ==================================================
    // Realtime (event-time replay, DB별 독립 watermark)
    // ==================================================
    private static void runRealtime(String basePath, KafkaSender sender) throws Exception {

        File[] dbFiles = new File(basePath)
                .listFiles(f -> f.getName().endsWith(".sqlite"));

        if (dbFiles == null || dbFiles.length == 0) {
            System.out.println("[Realtime] no sqlite files found");
            return;
        }

        // 🔑 DB별 lastSentTime
        Map<String, Instant> lastSentPerDb = new HashMap<>();

        System.out.println("[Realtime] event-time based replay (per DB)");

        while (true) {

            Instant now = Instant.now();

            for (File db : dbFiles) {

                String dbName = db.getName();
                Instant lastSent = lastSentPerDb.get(dbName);

                if (lastSent == null) {
                    // DB별 최초 시작 시점
                    lastSent = now.minusSeconds(10);
                    lastSentPerDb.put(dbName, lastSent);
                    System.out.println(
                        "[Realtime start] db=" + dbName + " from " + lastSent
                    );
                }

                try (Connection conn = SqliteClient.connect(db.getAbsolutePath())) {

                    RawLogRepository repo = new RawLogRepository(conn);
                    List<RawLogRow> rows = repo.findBetween(lastSent, now);

                    if (!rows.isEmpty()) {
                        System.out.printf(
                            "[Realtime] db=%s rows=%d (%s → %s)%n",
                            dbName,
                            rows.size(),
                            lastSent,
                            now
                        );
                    }

                    for (RawLogRow row : rows) {
                        sender.send(row);
                        lastSent = row.receivedAt;
                    }

                    // DB별 watermark 갱신
                    lastSentPerDb.put(dbName, lastSent);
                }
            }

            Thread.sleep(500);
        }
    }

    // ==================================================
    // Backfill (batch + flush + pacing)
    // ==================================================
    private static void runBackfill(String basePath, KafkaSender sender) throws Exception {

        File[] dbFiles = new File(basePath)
                .listFiles(f -> f.getName().endsWith(".sqlite"));

        if (dbFiles == null || dbFiles.length == 0) return;

        final int FLUSH_EVERY = 5_000;
        final int SLEEP_MS   = 5;

        for (File db : dbFiles) {

            System.out.println("[Backfill] " + db.getName());

            try (Connection conn = SqliteClient.connect(db.getAbsolutePath())) {

                RawLogRepository repo = new RawLogRepository(conn);
                List<RawLogRow> rows = repo.findAllOrdered();

                int sent = 0;

                for (RawLogRow row : rows) {
                    sender.send(row);
                    sent++;

                    if (sent % FLUSH_EVERY == 0) {
                        sender.flush();
                        Thread.sleep(SLEEP_MS);
                        System.out.printf(
                            "[Backfill] %s sent=%d%n",
                            db.getName(), sent
                        );
                    }
                }

                sender.flush();
                System.out.printf(
                    "[Backfill completed] %s total=%d%n",
                    db.getName(), sent
                );
            }
        }
    }

    // ==================================================
    // Utils
    // ==================================================
    private static String getenv(String key) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) {
            throw new RuntimeException(key + " not set");
        }
        return v;
    }

    private static String getenvOrDefault(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }
}
