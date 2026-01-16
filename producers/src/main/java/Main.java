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
    // Realtime (항상 실행, 자기 기준으로만)
    // ==================================================
    private static void runRealtime(String basePath, KafkaSender sender) throws Exception {

        File[] dbFiles = new File(basePath)
                .listFiles(f -> f.getName().endsWith(".sqlite"));

        if (dbFiles == null || dbFiles.length == 0) {
            System.out.println("[Realtime] no sqlite files found");
            return;
        }

        Map<String, Instant> lastSentPerDb = new HashMap<>();

        Instant realtimeStart = Instant.now();
        System.out.println("[Realtime start] T0=" + realtimeStart);

        while (true) {

            Instant now = Instant.now();

            for (File db : dbFiles) {

                String dbName = db.getName();
                Instant lastSent = lastSentPerDb.get(dbName);

                if (lastSent == null) {
                    lastSent = realtimeStart;
                    lastSentPerDb.put(dbName, lastSent);
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
    // Backfill (Realtime 시작 시점 이전까지만)
    // ==================================================
    private static void runBackfill(String basePath, KafkaSender sender) throws Exception {

        String cutoffStr = getenv("BACKFILL_CUTOFF");
        Instant cutoffTime = Instant.parse(cutoffStr);

        System.out.println("[Backfill cutoff] " + cutoffTime);

        File[] dbFiles = new File(basePath)
                .listFiles(f -> f.getName().endsWith(".sqlite"));

        if (dbFiles == null || dbFiles.length == 0) return;

        final int BATCH_SIZE = 1_000;
        final int LOG_EVERY  = 10_000;
        final int SLEEP_MS  = 5;

        class DbState {
            final String name;
            final Connection conn;
            final RawLogRepository repo;
            Instant cursor = Instant.EPOCH;
            long sent = 0;
            boolean finished = false;

            DbState(File db) throws Exception {
                this.name = db.getName();
                this.conn = SqliteClient.connect(db.getAbsolutePath());
                this.repo = new RawLogRepository(conn);
            }
        }

        List<DbState> states = new ArrayList<>();
        for (File db : dbFiles) {
            DbState s = new DbState(db);
            states.add(s);
            System.out.println("[Backfill start] " + s.name);
        }

        boolean running = true;

        while (running) {

            running = false;

            for (DbState state : states) {

                if (state.finished) continue;
                running = true;

                List<RawLogRow> rows =
                        state.repo.findBetween(state.cursor, cutoffTime, BATCH_SIZE);

                if (rows.isEmpty()) {
                    state.finished = true;
                    System.out.printf(
                        "[Backfill completed] %s total=%d%n",
                        state.name, state.sent
                    );
                    continue;
                }

                for (RawLogRow row : rows) {
                    sender.send(row);
                    state.cursor = row.receivedAt;
                    state.sent++;

                    if (state.sent % LOG_EVERY == 0) {
                        System.out.printf(
                            "[Backfill progress] %s sent=%d%n",
                            state.name, state.sent
                        );
                    }
                }
            }

            sender.flush();
            Thread.sleep(SLEEP_MS);
        }

        for (DbState s : states) {
            s.conn.close();
        }

        System.out.println("[Backfill] all DBs completed");
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
