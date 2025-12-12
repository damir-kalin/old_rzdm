package com.example.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import java.sql.Types;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Objects;

public class ContractorDebtPipeline {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = createExecutionEnvironment();

        String kafkaUser = System.getenv().getOrDefault("KAFKA_ADMIN_USER", "admin");
        String kafkaPass = System.getenv().getOrDefault("KAFKA_ADMIN_PASSWORD", "Q1w2e3r+");

        String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092"
        );

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "PLAIN")
                .setProperty(
                        "sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + kafkaUser + "\" password=\"" + kafkaPass + "\";"
                )
                .setTopics("sys__asb__esud_rzdm__contractor_debt__data")
                .setGroupId("flink-contractor-debt-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Contractor Debt Source")
                .name("Kafka Contractor Debt Source")
                .uid("kafka-contractor-debt-source");

        // Transform to RawData objects (just wrap the JSON string)
        DataStream<RawData> eventStream = stream.map(value -> {
            if (value == null || value.trim().isEmpty()) {
                System.err.println("Warning: Received null or empty message, skipping...");
                return null;
            }
            RawData rawData = new RawData();
            rawData.setValues(value.trim());
            rawData.setLoadDttm(Timestamp.valueOf(LocalDateTime.now()));
            rawData.setSrcSysId("asb");
            return rawData;
        }).filter(Objects::nonNull)
          .name("Wrap raw JSON")
          .uid("wrap-raw-json");

        String jdbcUrl = System.getenv().getOrDefault("STARROCKS_JDBC_URL", "jdbc:mysql://kube-starrocks-fe-service:9030/iceberg.rzdm");
        String jdbcUser = System.getenv().getOrDefault("STARROCKS_USER", "root");
        String jdbcPassword = System.getenv().getOrDefault("STARROCKS_PASSWORD", "Q1w2e3r+");

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchIntervalMs(200)
                .withBatchSize(1000)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(jdbcUser)
                .withPassword(jdbcPassword)
                .build();

        final int parameterCount = 3;

        String insertSql = "INSERT INTO iceberg.rzdm.stg_contractor_debt (" +
                    "`values`, load_dttm, src_sys_id" +
                ") VALUES (" +
                    String.join(", ", Collections.nCopies(parameterCount, "?")) +
                ")";

        JdbcStatementBuilder<RawData> statementBuilder = (statement, rawData) -> {
            int index = 1;
            setNullableString(statement, index++, rawData.getValues());
            setNullableTimestamp(statement, index++, rawData.getLoadDttm());
            setNullableString(statement, index++, rawData.getSrcSysId());
        };

        DataStream<RawData> filteredStream = eventStream
                .filter(Objects::nonNull)
                .name("Filter null contractor debt")
                .uid("filter-contractor-debt");

        JdbcSink<RawData> starRocksSink = JdbcSink.<RawData>builder()
                .withQueryStatement(insertSql, statementBuilder)
                .withExecutionOptions(executionOptions)
                .buildAtLeastOnce(connectionOptions);

        filteredStream
                .sinkTo(starRocksSink)
                .name("StarRocks JDBC Sink")
                .uid("contractor-debt-jdbc-sink")
                .setParallelism(1);

        env.execute("Contractor Debt Streaming Pipeline");
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        Configuration config = new Configuration();
        String restAddress = System.getenv().getOrDefault("FLINK_REST_ADDRESS", "flink-jobmanager");
        int restPort = Integer.parseInt(System.getenv().getOrDefault("FLINK_REST_PORT", "8081"));
        config.set(RestOptions.ADDRESS, restAddress);
        config.set(RestOptions.PORT, restPort);
        
        // Отключаем проверку утечки classloader для избежания ошибок при завершении
        config.setString("classloader.check-leaked-classloader", "false");
        
        // Настройка стратегии перезапуска: до 3 попыток с задержкой 10 секунд
        config.setString("restart-strategy.type", "fixed-delay");
        config.setString("restart-strategy.fixed-delay.attempts", "3");
        config.setString("restart-strategy.fixed-delay.delay", "10 s");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, ContractorDebtPipeline.class.getClassLoader());
        env.setParallelism(1);
        
        return env;
    }

    private static void setNullableString(java.sql.PreparedStatement statement, int index, String value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.VARCHAR);
        } else {
            statement.setString(index, value);
        }
    }

    private static void setNullableTimestamp(java.sql.PreparedStatement statement, int index, Timestamp value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.TIMESTAMP);
        } else {
            statement.setTimestamp(index, value);
        }
    }

    public static class RawData {
        private String values;
        private Timestamp loadDttm;
        private String srcSysId;

        public String getValues() { return values; }
        public void setValues(String values) { this.values = values; }
        public Timestamp getLoadDttm() { return loadDttm; }
        public void setLoadDttm(Timestamp loadDttm) { this.loadDttm = loadDttm; }
        public String getSrcSysId() { return srcSysId; }
        public void setSrcSysId(String srcSysId) { this.srcSysId = srcSysId; }
    }
}
