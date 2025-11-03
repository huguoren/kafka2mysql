package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
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

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;

public class MultiSinkFlinkJob {

    public static void main(String[] args) throws Exception {
        // 1. 解析命令行参数
        final ParameterTool params = ParameterTool.fromArgs(args);

        // 2. 创建执行环境并注册全局参数
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

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

        // 5. 按用户ID分组并计算行为计数
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

        // 6. 按行为类型分组并计算计数
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

//        // 8. Kafka Sink - 将行为类型统计发送到另一个Kafka Topic
//        KafkaSink<Tuple2<String, Integer>> kafkaSink = KafkaSink.<Tuple2<String, Integer>>builder()
//                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:9092"))
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                                             .setTopic(params.get("kafka.output.topic", "action-stats-topic"))
//                                             .setKeySerializationSchema((element) -> element.f0.getBytes(StandardCharsets.UTF_8))
//                                             .setValueSerializationSchema((element) ->
//                                                                                  String.format("{\"action\":\"%s\",\"count\":%d}", element.f0, element.f1)
//                                                                                          .getBytes(StandardCharsets.UTF_8))
//                                             .build()
//                )
//                .build();

        // 8. Kafka Sink - 将行为类型统计发送到另一个Kafka Topic
        KafkaSink<Tuple2<String, Integer>> kafkaSink = KafkaSink.<Tuple2<String, Integer>>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:9092"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<Tuple2<String, Integer>>builder()
                                .setTopic(params.get("kafka.output.topic", "action-stats-topic"))
//                                .setKeySerializationSchema(new SimpleStringSchema())
                                .setValueSerializationSchema(new SerializationSchema<Tuple2<String, Integer>>() {
                                    @Override
                                    public byte[] serialize(Tuple2<String, Integer> element) {
                                        String json = String.format("{\"action\":\"%s\",\"count\":%d}", element.f0, element.f1);
                                        return json.getBytes(StandardCharsets.UTF_8);
                                    }
                                })
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                // 可以添加Kafka生产者配置
                .setProperty("batch.size", "16384")
                .setProperty("linger.ms", "5")
                .setProperty("max.request.size", "1048576") // 1MB 默认值:cite[1]
                .build();

        actionTypeCounts.sinkTo(kafkaSink).name("Kafka Action Stats Sink");

        // 9. Redis Sink - 使用自定义Sink Function写入实时统计
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

                jedis = new Jedis(redisHost, redisPort);
                if (!redisPassword.isEmpty()) {
                    jedis.auth(redisPassword);
                }
            }

            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Void> out) {
                try {
                    // 使用Hash存储用户行为计数
                    String key = "user:action:stats";
                    String field = value.f0; // userId
                    String count = String.valueOf(value.f1);

                    jedis.hset(key, field, count);
                    // 设置过期时间（可选）
                    jedis.expire(key, 3600); // 1小时过期
                } catch (Exception e) {
                    System.err.println("Failed to write to Redis: " + e.getMessage());
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
        env.execute("Multi-Sink Flink Job: Kafka to MySQL, Kafka, Redis");
    }
}
