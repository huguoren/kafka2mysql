package com.example.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FinancialTradeMonitoring {

    public static void main(String[] args) throws Exception {
        // 设置流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建交易事件流（实际应用中从Kafka等消息队列读取）
        DataStream<TradeEvent> tradeStream = env
                .addSource(new TradeEventSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TradeEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(TradeEvent element) {
                        return element.getTimestamp();
                    }
                });

        // 按交易标的分组
        KeyedStream<TradeEvent, String> keyedStream = tradeStream
                .keyBy(TradeEvent::getSymbol);

        // 检测模式1：幌骗交易（高频下单撤单）
        DataStream<AlertEvent> spoofingAlerts = detectSpoofingPattern(keyedStream);

        // 检测模式2：趋势操纵
        DataStream<AlertEvent> manipulationAlerts = detectManipulationPattern(keyedStream);

        // 检测模式3：异常波动
        DataStream<AlertEvent> volatilityAlerts = detectVolatilityPattern(keyedStream);

        // 合并所有告警流并输出
        DataStream<AlertEvent> allAlerts = spoofingAlerts
                .union(manipulationAlerts)
                .union(volatilityAlerts);

        allAlerts.print("Financial Alerts");

        env.execute("Financial Trade Monitoring with FlinkCEP");
    }

    /**
     * 检测幌骗交易模式：短时间内高频率下单撤单
     */
    private static DataStream<AlertEvent> detectSpoofingPattern(
            KeyedStream<TradeEvent, String> keyedStream) {

        // 定义CEP模式：10秒内订单创建后快速撤销的模式
        Pattern<TradeEvent, ?> spoofingPattern = Pattern.<TradeEvent>begin("create_orders")
                .where(new SimpleCondition<TradeEvent>() {
                    @Override
                    public boolean filter(TradeEvent event) {
                        return "NEW".equals(event.getOrderType());
                    }
                })
                .timesOrMore(5)  // 至少5个新建订单
                .within(Time.seconds(10))
                .followedBy("cancel_orders")
                .where(new SimpleCondition<TradeEvent>() {
                    @Override
                    public boolean filter(TradeEvent event) {
                        return "CANCEL".equals(event.getOrderType());
                    }
                })
                .timesOrMore(4)  // 至少4个撤销订单
                .within(Time.seconds(5));

        // 在事件流上应用模式
        PatternStream<TradeEvent> patternStream = CEP.pattern(
                keyedStream,
                spoofingPattern
        );

        // 处理匹配的模式
        return patternStream.process(new PatternProcessFunction<TradeEvent, AlertEvent>() {
            @Override
            public void processMatch(
                    Map<String, List<TradeEvent>> pattern,
                    Context ctx,
                    Collector<AlertEvent> out) {

                List<TradeEvent> createOrders = pattern.get("create_orders");
                List<TradeEvent> cancelOrders = pattern.get("cancel_orders");

                double cancelRatio = (double) cancelOrders.size() / createOrders.size();

                if (cancelRatio > 0.8) {  // 撤单率超过80%
                    String symbol = createOrders.get(0).getSymbol();
                    String desc = String.format(
                            "Spoofing detected: %.0f%% cancellation rate (%d created, %d cancelled)",
                            cancelRatio * 100, createOrders.size(), cancelOrders.size()
                    );

                    List<TradeEvent> allEvents = new ArrayList<>();
                    allEvents.addAll(createOrders);
                    allEvents.addAll(cancelOrders);

                    AlertEvent alert = new AlertEvent(
                            "SPOOFING", symbol, desc, cancelRatio,
                            System.currentTimeMillis(), allEvents
                    );
                    out.collect(alert);
                }
            }
        });
    }

    /**
     * 检测趋势操纵模式：连续同方向大额交易推动价格
     */
    private static DataStream<AlertEvent> detectManipulationPattern(
            KeyedStream<TradeEvent, String> keyedStream) {

        Pattern<TradeEvent, ?> manipulationPattern = Pattern.<TradeEvent>begin("first_trade")
                .where(new SimpleCondition<TradeEvent>() {
                    @Override
                    public boolean filter(TradeEvent event) {
                        return "EXECUTE".equals(event.getOrderType()) &&
                                event.getQuantity() > 10000;  // 大额交易
                    }
                })
                .next("consecutive_trades")
                .where(new SimpleCondition<TradeEvent>() {
                    @Override
                    public boolean filter(TradeEvent event) {
                        return "EXECUTE".equals(event.getOrderType()) &&
                                event.getQuantity() > 5000;
                    }
                })
                .timesOrMore(4)  // 连续至少5笔大额交易
                .consecutive()
                .within(Time.seconds(5));

        return CEP.pattern(keyedStream, manipulationPattern)
                .process(new PatternProcessFunction<TradeEvent, AlertEvent>() {
                    @Override
                    public void processMatch(
                            Map<String, List<TradeEvent>> pattern,
                            Context ctx,
                            Collector<AlertEvent> out) {

                        List<TradeEvent> allTrades = new ArrayList<>();
                        pattern.values().forEach(allTrades::addAll);

                        // 检查是否同方向且价格持续变动
                        String firstDirection = allTrades.get(0).getDirection();
                        double firstPrice = allTrades.get(0).getPrice();
                        double lastPrice = allTrades.get(allTrades.size() - 1).getPrice();
                        double priceChange = Math.abs(lastPrice - firstPrice) / firstPrice;

                        boolean sameDirection = allTrades.stream()
                                .allMatch(trade -> firstDirection.equals(trade.getDirection()));

                        if (sameDirection && priceChange > 0.02) {  // 价格变动超过2%
                            String symbol = allTrades.get(0).getSymbol();
                            String desc = String.format(
                                    "Possible manipulation: %d consecutive %s trades, price change %.2f%%",
                                    allTrades.size(), firstDirection, priceChange * 100
                            );

                            AlertEvent alert = new AlertEvent(
                                    "MANIPULATION", symbol, desc, 0.85,
                                    System.currentTimeMillis(), allTrades
                            );
                            out.collect(alert);
                        }
                    }
                });
    }

    /**
     * 检测异常波动模式
     */
    private static DataStream<AlertEvent> detectVolatilityPattern(
            KeyedStream<TradeEvent, String> keyedStream) {

        Pattern<TradeEvent, TradeEvent> volatilityPattern = Pattern.<TradeEvent>begin("start")
                .where(new SimpleCondition<TradeEvent>() {
                    @Override
                    public boolean filter(TradeEvent event) {
                        return "EXECUTE".equals(event.getOrderType());
                    }
                })
                .timesOrMore(3)
                .within(Time.seconds(3));

        return CEP.pattern(keyedStream, volatilityPattern)
                .process(new PatternProcessFunction<TradeEvent, AlertEvent>() {
                    @Override
                    public void processMatch(
                            Map<String, List<TradeEvent>> pattern,
                            Context ctx,
                            Collector<AlertEvent> out) {

                        List<TradeEvent> trades = pattern.get("start");
                        if (trades.size() >= 3) {
                            double minPrice = trades.stream()
                                    .mapToDouble(TradeEvent::getPrice)
                                    .min().orElse(0.0);
                            double maxPrice = trades.stream()
                                    .mapToDouble(TradeEvent::getPrice)
                                    .max().orElse(0.0);

                            double volatility = (maxPrice - minPrice) / minPrice;

                            if (volatility > 0.03) {  // 波动率超过3%
                                String symbol = trades.get(0).getSymbol();
                                String desc = String.format(
                                        "High volatility: %.2f%% in %d trades",
                                        volatility * 100, trades.size()
                                );

                                AlertEvent alert = new AlertEvent(
                                        "VOLATILITY", symbol, desc, volatility,
                                        System.currentTimeMillis(), trades
                                );
                                out.collect(alert);
                            }
                        }
                    }
                });
    }
}
