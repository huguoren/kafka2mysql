package com.example.real;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.math.BigDecimal;
import java.util.Random;

public class TransactionSourceFunction implements SourceFunction<TransactionEvent> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private int count = 0;

    // 模拟用户ID
    private final String[] userIds = {"user1", "user2", "user3", "user4", "user5"};
    // 模拟交易渠道
    private final String[] channels = {"mobile", "web", "pos", "atm"};

    @Override
    public void run(SourceContext<TransactionEvent> ctx) throws Exception {
        while (isRunning && count < 1000) { // 生成1000条测试数据
            String userId = userIds[random.nextInt(userIds.length)];
            BigDecimal amount = new BigDecimal(random.nextInt(5000) + 100); // 100-5100元
            long timestamp = System.currentTimeMillis() - random.nextInt(60000); // 模拟1分钟内的数据

            TransactionEvent event = new TransactionEvent(
                    userId,
                    "txn_" + System.currentTimeMillis() + "_" + count,
                    amount,
                    timestamp,
                    channels[random.nextInt(channels.length)]
            );

            ctx.collect(event);
            count++;

            // 每秒产生2条数据
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
