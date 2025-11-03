package com.example;

import lombok.Data;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink + CEP 的完整示例 — 包含内存模拟 source 和 CEP 检测逻辑
 */
public class TradeAnomalyCEPWithSource {

    // 交易事件类
    @Data
    public static class TradeEvent {
        public String accountId;
        public long eventTime;
        public String eventType;   // "PLACED" 或 "CANCELED"
        public String direction;   // "BUY" or "SELL"
        public double price;
        public long quantity;

        public TradeEvent() {}

        public TradeEvent(String accountId, long eventTime, String eventType,
                          String direction, double price, long quantity) {
            this.accountId = accountId;
            this.eventTime = eventTime;
            this.eventType = eventType;
            this.direction = direction;
            this.price = price;
            this.quantity = quantity;
        }

        @Override
        public String toString() {
            return "TradeEvent{" +
                    "accountId='" + accountId + '\'' +
                    ", eventTime=" + eventTime +
                    ", eventType='" + eventType + '\'' +
                    ", direction='" + direction + '\'' +
                    ", price=" + price +
                    ", quantity=" + quantity +
                    '}';
        }
    }

    // 异常报警类
    @Data
    public static class AnomalyAlarm {
        public String accountId;
        public TradeEvent first;
        public TradeEvent second;
        public String reason;

        public AnomalyAlarm() {}

        public AnomalyAlarm(String accountId, TradeEvent first, TradeEvent second, String reason) {
            this.accountId = accountId;
            this.first = first;
            this.second = second;
            this.reason = reason;
        }

        @Override
        public String toString() {
            return "AnomalyAlarm{" +
                    "accountId='" + accountId + '\'' +
                    ", first=" + first +
                    ", second=" + second +
                    ", reason='" + reason + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        // 构造执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用 event time 语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 模拟内存事件源
        DataStream<TradeEvent> source = env.fromElements(
                        // accountId, eventTime (ms), eventType, direction, price, quantity
                        new TradeEvent("acct1", 1000L, "PLACED", "BUY", 10.0, 200),
                        new TradeEvent("acct1", 1150L, "CANCELED", "BUY", 10.0, 200),
                        new TradeEvent("acct2", 2000L, "PLACED", "SELL", 5.0, 150),
                        new TradeEvent("acct2", 2100L, "PLACED", "BUY", 5.0, 150),
                        new TradeEvent("acct3", 3000L, "PLACED", "BUY", 8.0, 50),
                        new TradeEvent("acct3", 3100L, "CANCELED", "BUY", 8.0, 50),
                        new TradeEvent("acct1", 4000L, "PLACED", "BUY", 200.0, 300),
                        new TradeEvent("acct1", 4300L, "PLACED", "SELL", 200.0, 300)
                )
                // 分配时间戳和 watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeEvent>forBoundedOutOfOrderness(Duration.ofMillis(50))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TradeEvent>() {
                                    @Override
                                    public long extractTimestamp(TradeEvent element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );

        // 按 accountId 分组
        KeyedStream<TradeEvent, String> keyed = source.keyBy(evt -> evt.accountId);

        // 定义 CEP 模式
        Pattern<TradeEvent, ?> pattern = Pattern.<TradeEvent>begin("first")
                .where(new IterativeCondition<TradeEvent>() {
                    @Override
                    public boolean filter(TradeEvent evt, Context<TradeEvent> ctx) {
                        // 先下单，且数量 ≥ 阈值（这里阈值设为 100）
                        return "PLACED".equals(evt.eventType) && evt.quantity >= 100;
                    }
                })
                .next("second")
                .where(new IterativeCondition<TradeEvent>() {
                    @Override
                    public boolean filter(TradeEvent evt, Context<TradeEvent> ctx) throws Exception {
                        // 获取 first 事件以用于比对方向
                        TradeEvent firstEvt = ctx.getEventsForPattern("first").iterator().next();
                        // 第二步可以是撤单，或者方向相反的下单
                        if ("CANCELED".equals(evt.eventType)) {
                            return true;
                        }
                        if ("PLACED".equals(evt.eventType) && !evt.direction.equals(firstEvt.direction)) {
                            return true;
                        }
                        return false;
                    }
                })
                .within(Time.milliseconds(200));  // 时间约束：200 ms

        // 应用 CEP 模式，生成报警
        SingleOutputStreamOperator<AnomalyAlarm> alarms = CEP
                .pattern(keyed, pattern)
                .select(new PatternSelectFunction<TradeEvent, AnomalyAlarm>() {
                    @Override
                    public AnomalyAlarm select(Map<String, List<TradeEvent>> pattern) throws Exception {
                        TradeEvent first = pattern.get("first").get(0);
                        TradeEvent second = pattern.get("second").get(0);
                        return new AnomalyAlarm(first.accountId, first, second, "Rapid cancel or reverse");
                    }
                });

        // 输出报警
        alarms.print();

        // 启动执行
        env.execute("Flink CEP Trade Anomaly Demo");
    }
}

