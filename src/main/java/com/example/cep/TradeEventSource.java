package com.example.cep;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

// 模拟交易事件数据源
public class TradeEventSource implements SourceFunction<TradeEvent> {
    private volatile boolean isRunning = true;
    private Random random = new Random();

    private final String[] symbols = {"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"};
    private final String[] directions = {"BUY", "SELL"};
    private final String[] orderTypes = {"NEW", "CANCEL", "EXECUTE"};

    @Override
    public void run(SourceContext<TradeEvent> ctx) throws Exception {
        long timestamp = System.currentTimeMillis();
        int eventCount = 0;

        while (isRunning && eventCount < 1000) {  // 生成1000个测试事件
            String symbol = symbols[random.nextInt(symbols.length)];
            String direction = directions[random.nextInt(directions.length)];
            String orderType = orderTypes[random.nextInt(orderTypes.length)];

            // 模拟价格：50-500之间
            double price = 50 + random.nextDouble() * 450;
            // 模拟数量：1000-50000
            double quantity = 1000 + random.nextDouble() * 49000;

            // 偶尔生成大额交易
            if (random.nextDouble() < 0.1) {
                quantity = 50000 + random.nextDouble() * 100000;
            }

            TradeEvent event = new TradeEvent(
                    "TRADE-" + eventCount,
                    symbol,
                    price,
                    quantity,
                    direction,
                    orderType,
                    timestamp,
                    "TRADER-" + random.nextInt(10)
            );

            ctx.collect(event);
            eventCount++;

            // 模拟事件时间推进
            timestamp += random.nextInt(1000);  // 0-1秒间隔

            // 控制事件生成速度
            Thread.sleep(50);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
