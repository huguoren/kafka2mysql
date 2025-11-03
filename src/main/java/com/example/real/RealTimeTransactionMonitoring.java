package com.example.real;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.time.Duration;

public class RealTimeTransactionMonitoring {

    // 定义侧输出流标签用于捕获迟到数据
    private static final OutputTag<TransactionEvent> LATE_DATA_TAG = new OutputTag<TransactionEvent>("late-data") {
    };

    public static void main(String[] args) throws Exception {

        // 在main方法开始处添加
        System.out.println("Java library path: " + System.getProperty("java.library.path"));
        System.out.println("Temp directory: " + System.getProperty("java.io.tmpdir"));

        // 1. 创建执行环境
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
//        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
//        config.set(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE, MemorySize.parse("128", MemorySize.MemoryUnit.MEGA_BYTES));
//        config.set(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER, 4);
//        config.set(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE, MemorySize.parse("256", MemorySize.MemoryUnit.MEGA_BYTES));
//        config.set(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, true);

        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///C:\\Users\\Venice\\AppData\\Local\\Temp\\flink-checkpoints");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(10000));
        config.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
        config.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMillis(60000));
        config.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofMillis(500));
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        // 启用Checkpoint，每10秒一次（生产环境建议配置）
        // env.enableCheckpointing(10000);
        // === 新增：配置RocksDB状态后端和检查点 ===
        // 1. 设置状态后端为RocksDB，并开启增量检查点
//        String checkpointPath = "file:///tmp/flink-checkpoints"; // 生产环境建议用HDFS或S3
//        RocksDBStateBackend rocksDBBackend = new RocksDBStateBackend(checkpointPath, true);
//        env.setStateBackend(rocksDBBackend);

        // 2. 启用检查点，间隔10秒，精确一次语义
//        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000); // 检查点超时时间
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); // 检查点间最小间隔
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 最大并发检查点数
        // 可选：任务取消时保留检查点
        // env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 3. （可选）通过配置对象设置一些RocksDB高级参数 (以下为示例，可按需调整)
//        Configuration config = new Configuration();
//        config.setString("state.backend.rocksdb.writebuffer.size", "128 MB");
//        config.setString("state.backend.rocksdb.writebuffer.count", "4");
//        config.setString("state.backend.rocksdb.block.cache-size", "256 MB");
//        config.setString("state.backend.rocksdb.compaction.level.use-dynamic-size", "true"); // 开启动态level大小[citation:6]
//        env.configure(config);

        // 2. 创建模拟交易数据流
        DataStream<TransactionEvent> transactionStream = env.addSource(new TransactionSourceFunction())
                .name("transaction-source");

        // 3. 分配时间戳和水位线
        SingleOutputStreamOperator<TransactionEvent> timedStream = transactionStream
                .assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                )
                .name("timestamp-assigner");

        // 4. 窗口计算：按用户分组，1分钟滚动窗口，允许10秒延迟
        SingleOutputStreamOperator<TransactionAlert> alerts = timedStream
                .keyBy(TransactionEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(Duration.ofSeconds(10))
                .sideOutputLateData(LATE_DATA_TAG)
                .aggregate(new TransactionAggregator(), new AlertProcessFunction())
                .name("transaction-window");

        // 5. 输出主流结果（告警信息）
        alerts.print("Real-Time Alerts");

        // 6. 处理迟到数据（侧输出流）
        DataStream<TransactionEvent> lateDataStream = alerts.getSideOutput(LATE_DATA_TAG);
        lateDataStream.print("Late Data");

        // 7. 执行任务
        env.execute("Real-Time Transaction Monitoring Demo");
    }

    // 交易数据聚合器
    public static class TransactionAggregator implements AggregateFunction<TransactionEvent, Tuple2<BigDecimal, Integer>, Tuple2<BigDecimal, Integer>> {

        @Override
        public Tuple2<BigDecimal, Integer> createAccumulator() {
            return Tuple2.of(BigDecimal.ZERO, 0);
        }

        @Override
        public Tuple2<BigDecimal, Integer> add(TransactionEvent value, Tuple2<BigDecimal, Integer> accumulator) {
            return Tuple2.of(accumulator.f0.add(value.getAmount()), accumulator.f1 + 1);
        }

        @Override
        public Tuple2<BigDecimal, Integer> getResult(Tuple2<BigDecimal, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<BigDecimal, Integer> merge(Tuple2<BigDecimal, Integer> a, Tuple2<BigDecimal, Integer> b) {
            return Tuple2.of(a.f0.add(b.f0), a.f1 + b.f1);
        }
    }

    // 告警处理函数
    public static class AlertProcessFunction extends ProcessWindowFunction<Tuple2<BigDecimal, Integer>, TransactionAlert, String, TimeWindow> {

        private static final BigDecimal THRESHOLD = new BigDecimal("10000.00");

        @Override
        public void process(String userId, Context context, Iterable<Tuple2<BigDecimal, Integer>> elements, Collector<TransactionAlert> out) {

            Tuple2<BigDecimal, Integer> result = elements.iterator().next();
            BigDecimal totalAmount = result.f0;
            int transactionCount = result.f1;

            // 检查是否超过阈值
            if (totalAmount.compareTo(THRESHOLD) > 0) {
                String alertMsg = String.format("User %s exceeded threshold: total %s in %d transactions", userId, totalAmount, transactionCount);

                TransactionAlert alert = new TransactionAlert(userId, totalAmount, context.window().getStart(), context.window().getEnd(), alertMsg);

                out.collect(alert);
            }
        }
    }
}
