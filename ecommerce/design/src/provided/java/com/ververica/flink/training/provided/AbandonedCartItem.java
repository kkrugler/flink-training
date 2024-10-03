package com.ververica.flink.training.provided;

public class AbandonedCartItem {
    private String transactionId;
    private long transactionTime;
    private String customerId;
    private String productId;

    public AbandonedCartItem() {}

    public AbandonedCartItem(String transactionId, long transactionTime, String customerId, String productId) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.transactionTime = transactionTime;
        this.productId = productId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public long getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(long transactionTime) {
        this.transactionTime = transactionTime;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }
}
