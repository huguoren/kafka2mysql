package com.example.cep;

import lombok.Data;

import java.util.List;
import java.util.UUID;

// 异常检测结果
@Data
public class AlertEvent {
    private String alertId;
    private String alertType;   // SPOOFING, MANIPULATION, VOLATILITY
    private String symbol;
    private String description;
    private Double confidence;  // 置信度
    private Long timestamp;
    private List<TradeEvent> relatedTrades;

    // 构造函数、getter、setter
    public AlertEvent(String alertType, String symbol, String description,
                      Double confidence, Long timestamp, List<TradeEvent> relatedTrades) {
        this.alertId = UUID.randomUUID().toString();
        this.alertType = alertType;
        this.symbol = symbol;
        this.description = description;
        this.confidence = confidence;
        this.timestamp = timestamp;
        this.relatedTrades = relatedTrades;
    }

    // getters and setters...
    @Override
    public String toString() {
        return String.format("AlertEvent{type=%s, symbol=%s, desc=%s, confidence=%.2f}",
                             alertType, symbol, description, confidence);
    }
}
