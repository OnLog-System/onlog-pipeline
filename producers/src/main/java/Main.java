import kafka.KafkaProducerFactory;
import kafka.KafkaSender;
import model.RawLogRow;
import sqlite.RawLogRepository;
import sqlite.SqliteClient;

import java.io.File;
import java.sql.Connection;
import java.time.Instant;
import java.util.*;

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

        Map<String, Instant> lastSentPerDb = new HashMap<>();

        System.out.println("[Realtime] event-time based replay (per DB)");

        while (true) {

            Instant now = Instant.now();

            for (File db : dbFiles) {

                String dbName = db.getName();
                Instant lastSent = lastSentPerDb.get(dbName);

                if (lastSent == null) {
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
                            dbName, rows.size(), lastSent, now
                        );
                    }

                    for (RawLogRow row : rows) {
                        sender.send(row);
                        lastSent = row.receivedAt;
                    }

                    lastSentPerDb.put(dbName, lastSent);
                }
            }

            Thread.sleep(500);
        }
    }

    // ==================================================
    // Backfill (multi-DB interleaved replay)
    // ==================================================
    private static void runBackfill(String basePath, KafkaSender sender) throws Exception {

        File[] dbFiles = new File(basePath)
                .listFiles(f -> f.getName().endsWith(".sqlite"));

        if (dbFiles == null || dbFiles.length == 0) return;

        final int BATCH_PER_DB = 1000;   // DB당 한 번에 보낼 row 수
        final int SLEEP_MS = 5;

        class DbState {
            final String name;
            final Connection conn;
            final RawLogRepository repo;
            Instant lastSent = null;
            boolean finished = false;

            DbState(File db) throws Exception {
                this.name = db.getName();
                this.conn = SqliteClient.connect(db.getAbsolutePath());
                this.repo = new RawLogRepository(conn);
            }
        }

        List<DbState> states = new ArrayList<>();
        for (File db : dbFiles) {
            states.add(new DbState(db));
            System.out.println("[Backfill start] " + db.getName());
        }

        while (true) {

            boolean allFinished = true;

            for (DbState state : states) {

                if (state.finished) continue;
                allFinished = false;

                Instant from = (state.lastSent == null)
                        ? Instant.EPOCH
                        : state.lastSent;

                List<RawLogRow> rows =
                        state.repo.findBetween(from, Instant.now());

                if (rows.isEmpty()) {
                    state.finished = true;
                    System.out.println("[Backfill completed] " + state.name);
                    continue;
                }

                int sent = 0;
                for (RawLogRow row : rows) {
                    sender.send(row);
                    state.lastSent = row.receivedAt;
                    sent++;

                    if (sent >= BATCH_PER_DB) break;
                }
            }

            sender.flush();
            Thread.sleep(SLEEP_MS);

            if (allFinished) break;
        }

        for (DbState state : states) {
            state.conn.close();
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
