package com.example.real;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class TransactionEvent {
    private String userId;
    private String transactionId;
    private BigDecimal amount;
    private Long timestamp;
    private String channel;

    // 构造函数
    public TransactionEvent() {}

    public TransactionEvent(String userId, String transactionId, BigDecimal amount, Long timestamp, String channel) {
        this.userId = userId;
        this.transactionId = transactionId;
        this.amount = amount;
        this.timestamp = timestamp;
        this.channel = channel;
    }

    // Getter和Setter方法
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "userId='" + userId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + new Timestamp(timestamp) +
                ", channel='" + channel + '\'' +
                '}';
    }
}
