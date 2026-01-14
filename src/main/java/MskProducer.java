import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MskProducer {

    public static void main(String[] args) throws Exception {

        // ===== ENV =====
        String bootstrapServers = getenvOrFail("KAFKA_BOOTSTRAP_SERVERS");
        String dbBasePath = getenvOrFail("DB_BASE_PATH");
        String topicPrefix = System.getenv().getOrDefault("TOPIC_PREFIX", "onlog");
        // ===============

        // ===== Kafka Props =====
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // IAM + TLS
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config",
                "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        System.out.println("Scanning DBs under: " + dbBasePath);

        while (true) {
            List<File> dbFiles = scanSqliteFiles(dbBasePath);

            for (File db : dbFiles) {
                String dbName = db.getName(); // e.g. F02_sensor_env.sqlite
                String topic = buildTopic(topicPrefix, dbName);

                // 지금은 DB 읽기 대신 "존재 확인 이벤트"만 보냄
                String payload = String.format(
                        "{\"db\":\"%s\",\"size\":%d}",
                        dbName, db.length()
                );

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, dbName, payload);

                producer.send(record, (meta, ex) -> {
                    if (ex != null) {
                        ex.printStackTrace();
                    } else {
                        System.out.printf(
                                "SENT topic=%s partition=%d offset=%d%n",
                                meta.topic(), meta.partition(), meta.offset()
                        );
                    }
                });
            }

            producer.flush();
            TimeUnit.SECONDS.sleep(10);
        }
    }

    // ---------- helpers ----------

    private static String getenvOrFail(String key) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) {
            throw new RuntimeException(key + " is not set");
        }
        return v;
    }

    private static List<File> scanSqliteFiles(String basePath) throws Exception {
        File base = new File(basePath);
        if (!base.exists()) return Collections.emptyList();

        List<File> result = new ArrayList<>();
        Files.walk(base.toPath())
                .filter(p -> p.toString().endsWith(".sqlite"))
                .forEach(p -> result.add(p.toFile()));
        return result;
    }

    private static String buildTopic(String prefix, String dbName) {
        // F02_sensor_env.sqlite → onlog.sensor_env
        if (dbName.contains("sensor_env")) return prefix + ".sensor_env";
        if (dbName.contains("sensor_scale")) return prefix + ".sensor_scale";
        if (dbName.contains("machine")) return prefix + ".machine";
        return prefix + ".unknown";
    }
}
