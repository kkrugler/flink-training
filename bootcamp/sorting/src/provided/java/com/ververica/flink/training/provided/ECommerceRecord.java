package com.ververica.flink.training.solutions;

/*
 * A version of ShoppingCartRecord that only has the fields we use.
 */
public class ECommerceRecord {
    // Fields we sort on.
    private String country;
    private String paymentMethod;

    // Other fields that we print
    private String transactionId;
    private long transactionTime;
    private String customerId;
    private String shippingAddress;
    private double shippingCost;
    private String couponCode;

    public ECommerceRecord() { }

    public ECommerceRecord(ECommerceRecord base) {
        setCountry(base.getCountry());
        setPaymentMethod(base.getPaymentMethod());
        setTransactionId(base.getTransactionId());
        setTransactionTime(base.getTransactionTime());
        setCustomerId(base.getCustomerId());
        setShippingAddress(base.getShippingAddress());
        setShippingCost(base.getShippingCost());
        setCouponCode(base.getCouponCode());
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public long getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(long transactionTime) {
        this.transactionTime = transactionTime;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getShippingAddress() {
        return shippingAddress;
    }

    public void setShippingAddress(String shippingAddress) {
        this.shippingAddress = shippingAddress;
    }

    public double getShippingCost() {
        return shippingCost;
    }

    public void setShippingCost(double shippingCost) {
        this.shippingCost = shippingCost;
    }

    public String getCouponCode() {
        return couponCode;
    }

    public void setCouponCode(String couponCode) {
        this.couponCode = couponCode;
    }
}
