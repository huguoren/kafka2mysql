package com.example.cep;

import lombok.Data;

// 交易事件数据模型
@Data
public class TradeEvent {
    private String tradeId;
    private String symbol;      // 交易标的
    private Double price;       // 价格
    private Double quantity;    // 数量
    private String direction;   // 方向: BUY/SELL
    private String orderType;   // 订单类型: NEW/CANCEL/EXECUTE
    private Long timestamp;     // 事件时间
    private String traderId;    // 交易员ID

    // 构造函数、getter、setter
    public TradeEvent() {}

    public TradeEvent(String tradeId, String symbol, Double price, Double quantity,
                      String direction, String orderType, Long timestamp, String traderId) {
        this.tradeId = tradeId;
        this.symbol = symbol;
        this.price = price;
        this.quantity = quantity;
        this.direction = direction;
        this.orderType = orderType;
        this.timestamp = timestamp;
        this.traderId = traderId;
    }

    // getters and setters...
    @Override
    public String toString() {
        return String.format("TradeEvent{symbol=%s, price=%.2f, qty=%.0f, direction=%s, type=%s, time=%d}",
                             symbol, price, quantity, direction, orderType, timestamp);
    }
}
