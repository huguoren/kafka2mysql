package com.example.real;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class TransactionAlert {
    private String userId;
    private BigDecimal totalAmount;
    private Long windowStart;
    private Long windowEnd;
    private String alertMessage;
    private Long alertTime;

    public TransactionAlert() {}

    public TransactionAlert(String userId, BigDecimal totalAmount, Long windowStart, Long windowEnd, String alertMessage) {
        this.userId = userId;
        this.totalAmount = totalAmount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.alertMessage = alertMessage;
        this.alertTime = System.currentTimeMillis();
    }

    // Getter和Setter方法
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }

    public Long getWindowStart() { return windowStart; }
    public void setWindowStart(Long windowStart) { this.windowStart = windowStart; }

    public Long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Long windowEnd) { this.windowEnd = windowEnd; }

    public String getAlertMessage() { return alertMessage; }
    public void setAlertMessage(String alertMessage) { this.alertMessage = alertMessage; }

    public Long getAlertTime() { return alertTime; }
    public void setAlertTime(Long alertTime) { this.alertTime = alertTime; }

    @Override
    public String toString() {
        return "🚨 TransactionAlert{" +
                "userId='" + userId + '\'' +
                ", totalAmount=" + totalAmount +
                ", window=[" + new Timestamp(windowStart) + " - " + new Timestamp(windowEnd) + "]" +
                ", message='" + alertMessage + '\'' +
                ", alertTime=" + new Timestamp(alertTime) +
                '}';
    }
}
