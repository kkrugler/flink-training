package com.ververica.flink.training.common;

public class ShoppingCartRecord {
    private String transactionId;
    private String country;
    private boolean transactionCompleted;
    private long transactionTime;
    private String customerId;
    private String paymentMethod;
    private String shippingAddress;
    private double shippingCost;
    private String couponCode;
    private SimpleList<CartItem> items;


    public ShoppingCartRecord() {
        items = new SimpleList<>();
    }

    public ShoppingCartRecord(ShoppingCartRecord clone) {
        setCustomerId(clone.getCustomerId());
        setCountry(clone.getCountry());
        setCouponCode(clone.getCouponCode());
        setItems(new SimpleList<>(clone.getItems()));
        setPaymentMethod(clone.getPaymentMethod());
        setShippingAddress(clone.getShippingAddress());
        setShippingCost(clone.getShippingCost());
        setTransactionCompleted(clone.isTransactionCompleted());
        setTransactionTime(clone.getTransactionTime());

        for (int i = 0; i < items.getSize(); i++) {
            items.set(i, new CartItem(items.get(i)));
        }
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public boolean isTransactionCompleted() {
        return transactionCompleted;
    }

    public void setTransactionCompleted(boolean transactionCompleted) {
        this.transactionCompleted = transactionCompleted;
    }

    public long getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(long transactionTime) {
        this.transactionTime = transactionTime;
    }

    public SimpleList<CartItem> getItems() {
        return items;
    }

    public void setItems(SimpleList<CartItem> items) {
        this.items = items;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
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

    @Override
    public String toString() {
        return String.format("%s at %d with %d items", transactionId, transactionTime, items.getSize());
    }
}
