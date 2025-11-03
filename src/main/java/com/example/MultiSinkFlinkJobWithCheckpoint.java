package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;

public class MultiSinkFlinkJobWithCheckpoint {

    public static void main(String[] args) throws Exception {
        // 1. 解析命令行参数
        final ParameterTool params = ParameterTool.fromArgs(args);

        // 2. 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        Configuration config = new Configuration();
        // 选项1：HashMapStateBackend（适合状态量较小的情况）
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///flink/checkpoints");
        env.configure(config);



        //        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        // 选项2：EmbeddedRocksDBStateBackend（适合大状态场景）
//        EmbeddedRocksDBStateBackend rocksDBBackend = new EmbeddedRocksDBStateBackend();
//        rocksDBBackend.setDbStoragePath(params.get("rocksdb.dir", "file:///tmp/flink-rocksdb"));
//        env.setStateBackend(rocksDBBackend);
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointDir));



        // ============================
        // Checkpoint 和状态后端配置
        // ============================

//        // 启用检查点，间隔10秒
//        env.enableCheckpointing(10000);
//
//        // 高级Checkpoint配置
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//
//        // 设置Checkpoint模式：精确一次
//        checkpointConfig.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
//
//        // 设置Checkpoint超时时间
//        checkpointConfig.setCheckpointTimeout(60000);
//
//        // 设置同时进行的Checkpoint最大数量
//        checkpointConfig.setMaxConcurrentCheckpoints(2);
//
//        // 设置两次Checkpoint之间的最小间隔
//        checkpointConfig.setMinPauseBetweenCheckpoints(5000);
//
//        // 启用外部化Checkpoint，在作业取消时保留Checkpoint数据
//        checkpointConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

//        // 配置RocksDB状态后端
//        try {
//            // 设置RocksDB状态后端
//            String checkpointDir = params.get("checkpoint.dir", "file:///tmp/flink-checkpoints");
//            String rocksDbDir = params.get("rocksdb.dir", "file:///tmp/flink-rocksdb");
//
//            RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
//                    checkpointDir,
//                    true
//            );
//
//            // 设置RocksDB的本地存储目录
//            rocksDBStateBackend.setDbStoragePath(rocksDbDir);
//
//            // 配置RocksDB内存管理
//            Configuration rocksDBConfig = new Configuration();
//            rocksDBConfig.setString("state.backend.rocksdb.memory.managed", "true");
//            rocksDBConfig.setString("state.backend.rocksdb.memory.fixed-per-slot", "50mb");
//
//            env.setStateBackend(rocksDBStateBackend);
//
//        } catch (Exception e) {
//            System.err.println("Failed to configure RocksDB state backend: " + e.getMessage());
//            // 回退到默认状态后端
//            System.out.println("Falling back to default state backend");
//        }
//
//        // 配置重启策略
//        env.setRestartStrategy(
//                RestartStrategies.fixedDelayRestart(
//                        3, // 最多重启3次
//                        Time.of(10, TimeUnit.SECONDS) // 每次重启间隔10秒
//                )
//        );

        // 3. 配置Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:9092"))
                .setTopics(params.get("kafka.input.topic", "input-topic"))
                .setGroupId(params.get("kafka.consumer.group.id", "multi-sink-flink-group"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 4. 数据解析和转换
        SingleOutputStreamOperator<Tuple3<String, String, Long>> parsedStream = kafkaStream
                .flatMap(new FlatMapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, String, Long>> out) {
                        try {
                            String[] parts = value.split(",");
                            if (parts.length >= 3) {
                                String userId = parts[0];
                                String action = parts[1];
                                Long timestamp = Long.parseLong(parts[2]);
                                out.collect(new Tuple3<>(userId, action, timestamp));
                            }
                        } catch (Exception e) {
                            System.err.println("Failed to parse record: " + value);
                        }
                    }
                })
                .name("data-parser");

        // 5. 按用户ID分组并计算行为计数（有状态操作）
        SingleOutputStreamOperator<Tuple2<String, Integer>> userActionCounts = parsedStream
                .map(new MapFunction<Tuple3<String, String, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, String, Long> value) {
                        return Tuple2.of(value.f0, 1); // (userId, 1)
                    }
                })
                .keyBy(value -> value.f0)
                .sum(1)
                .name("user-action-counter");

        // 6. 按行为类型分组并计算计数（有状态操作）
        SingleOutputStreamOperator<Tuple2<String, Integer>> actionTypeCounts = parsedStream
                .map(new MapFunction<Tuple3<String, String, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, String, Long> value) {
                        return Tuple2.of(value.f1, 1); // (actionType, 1)
                    }
                })
                .keyBy(value -> value.f0)
                .sum(1)
                .name("action-type-counter");

        // ============================
        // 多目标Sink配置
        // ============================

        // 7. MySQL Sink - 写入用户行为统计
        userActionCounts.addSink(
                JdbcSink.sink(
                        "INSERT INTO user_action_stats (user_id, action_count, update_time) VALUES (?, ?, NOW()) " +
                                "ON DUPLICATE KEY UPDATE action_count = ?, update_time = NOW()",
                        (PreparedStatement ps, Tuple2<String, Integer> tuple) -> {
                            ps.setString(1, tuple.f0);
                            ps.setInt(2, tuple.f1);
                            ps.setInt(3, tuple.f1);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(params.getInt("mysql.batch.size", 100))
                                .withBatchIntervalMs(params.getLong("mysql.batch.interval.ms", 1000L))
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(params.get("mysql.jdbc.url", "jdbc:mysql://localhost:3306/flink_db"))
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername(params.get("mysql.username", "flink_user"))
                                .withPassword(params.get("mysql.password", "flink_password"))
                                .build()
                )
        ).name("MySQL User Stats Sink");

        // 8. Kafka Sink - 使用自定义序列化器
        KafkaSink<Tuple2<String, Integer>> kafkaSink = KafkaSink.<Tuple2<String, Integer>>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:9092"))
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Tuple2<String, Integer>>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(
                                    Tuple2<String, Integer> element,
                                    KafkaSinkContext context,
                                    Long timestamp) {

                                String actionType = element.f0;
                                Integer count = element.f1;

                                String jsonValue = String.format(
                                        "{\"actionType\":\"%s\",\"count\":%d,\"timestamp\":%d}",
                                        actionType, count, System.currentTimeMillis()
                                );

                                return new ProducerRecord<>(
                                        params.get("kafka.output.topic", "action-stats-topic"),
                                        null,
                                        null,
                                        actionType.getBytes(StandardCharsets.UTF_8),
                                        jsonValue.getBytes(StandardCharsets.UTF_8)
                                );
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                // 对于精确一次语义，需要配置事务前缀
                .setProperty("transaction.timeout.ms", "900000")
                .build();

        actionTypeCounts.sinkTo(kafkaSink).name("Kafka Action Stats Sink");

        // 9. Redis Sink - 使用连接池的改进版本
        userActionCounts.flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Void>() {
            private transient Jedis jedis;
            private String redisHost;
            private int redisPort;
            private String redisPassword;

            @Override
            public void open(Configuration parameters) {
                ParameterTool params = (ParameterTool) getRuntimeContext().getGlobalJobParameters();
                redisHost = params.get("redis.host", "localhost");
                redisPort = params.getInt("redis.port", 6379);
                redisPassword = params.get("redis.password", "");

                // 在open方法中初始化连接，确保连接在checkpoint时是稳定的
                jedis = new Jedis(redisHost, redisPort);
                if (!redisPassword.isEmpty()) {
                    jedis.auth(redisPassword);
                }

                // 测试连接
                try {
                    jedis.ping();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to connect to Redis", e);
                }
            }

            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Void> out) {
                try {
                    String key = "user:action:stats";
                    String field = value.f0;
                    String count = String.valueOf(value.f1);

                    // 使用管道提高性能
                    jedis.hset(key, field, count);
                    jedis.expire(key, 3600); // 1小时过期

                } catch (Exception e) {
                    System.err.println("Failed to write to Redis: " + e.getMessage());
                    // 在实际生产环境中，这里应该记录更详细的错误信息或指标
                }
            }

            @Override
            public void close() {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }).name("Redis User Stats Sink");

        // 10. 执行任务
        env.execute("Multi-Sink Flink Job with Checkpoint and RocksDB");
    }
}
