package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

@Slf4j
public class KafkaToMySQLJob2 {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启检查点，对于需要精确一次语义的Sink（如Kafka Producer）很重要
        // env.enableCheckpointing(10000); // 每10秒一次Checkpoint
        ParameterTool commandLineParams = ParameterTool.fromArgs(args);
        ParameterTool configFileParams = ParameterTool.fromPropertiesFile("conf/application.properties");
        ParameterTool parameters = commandLineParams.mergeWith(configFileParams);

        env.getConfig().setGlobalJobParameters(parameters);

        Configuration config = new Configuration();
        // 选项1：HashMapStateBackend（适合状态量较小的情况）
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///flink/checkpoints");
        env.configure(config);

        // 2. 配置Kafka Source (Flink 1.20.x 推荐使用新的Source API)
        String bootstrapServers = parameters.get("kafka.source.bootstrap-servers");
        String topics = parameters.get("kafka.source.topics");
        String groupId = parameters.get("kafka.source.consumer-group");
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 使用新的Source API读取数据
        DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3. 数据转换：解析字符串 (例如: "user1,login,2023-...")
        DataStream<Tuple3<String, String, Long>> parsedStream = kafkaStream.flatMap(new FlatMapFunction<String, Tuple3<String, String, Long>>() {
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
                    // 处理解析异常
                    System.err.println("Failed to parse record: " + value);
                }
            }
        });

        // 4. 分流 (Split) - 按行为类型过滤
        //   注意：Flink的split API已被标记为过时，推荐使用侧输出流或filter
        //   这里使用多个filter实现分流

        // 分流1: 登录行为
        DataStream<Tuple3<String, String, Long>> loginStream = parsedStream.filter(
                (FilterFunction<Tuple3<String, String, Long>>) value -> "login".equals(value.f1)).name("login-filter");

        // 分流2: 购买行为
        DataStream<Tuple3<String, String, Long>> purchaseStream = parsedStream.filter(
                (FilterFunction<Tuple3<String, String, Long>>) value -> "purchase".equals(value.f1)).name("purchase-filter");

        // 5. 分别处理不同的流 (示例：登录流按用户KeyBy并计数)
        DataStream<Tuple2<String, Integer>> loginCounts = loginStream.map(new MapFunction<Tuple3<String, String, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, String, Long> value) {
                        return Tuple2.of(value.f0, 1); // (userId, 1)
                    }
                }).keyBy(value -> value.f0) // 按userId分组
                .sum(1); // 对计数求和

        // 6. 合流 (Union) - 将多个结构相同的流合并
        // 先将购买流映射成与登录计数流相似的结构 (这里仅为示例，实际逻辑可能不同)
        DataStream<Tuple2<String, Integer>> purchaseCounts = purchaseStream.map(new MapFunction<Tuple3<String, String, Long>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<String, String, Long> value) {
                return Tuple2.of(value.f0 + "_purchase", 1); // 使用不同的key以示区别
            }
        }).keyBy(value -> value.f0).sum(1);

        // 使用union合流 (要求流的数据类型一致)
        DataStream<Tuple2<String, Integer>> mergedStream = loginCounts.union(purchaseCounts);

        // 7. 将处理后的数据写入MySQL (这里以合并后的流为例)
        String driver = parameters.get("trade.jdbc.driver-class-name");
        String url = parameters.get("trade.jdbc.url");
        String username = parameters.get("trade.jdbc.username");
        String password = parameters.get("trade.jdbc.password");
        mergedStream.addSink(
                JdbcSink.sink(
                "INSERT INTO user_actions (user_key, action_count) VALUES (?, ?) ON DUPLICATE KEY UPDATE action_count = ?",
                (preparedStatement, tuple) -> {
                    preparedStatement.setString(1, tuple.f0); // user_key
                    preparedStatement.setInt(2, tuple.f1);   // action_count
                    preparedStatement.setInt(3, tuple.f1);   // 用于ON DUPLICATE KEY UPDATE
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driver)
                        .withUsername(username)
                        .withPassword(password)
                        .build())
        ).name("MySQL Sink");

        // 8. 执行任务
        env.execute("Kafka to MySQL with Split and Union");
    }
}
