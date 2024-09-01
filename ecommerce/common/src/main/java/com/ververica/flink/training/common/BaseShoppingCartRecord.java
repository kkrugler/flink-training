package com.ververica.flink.training.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class that makes it easier for exercises to create specific versions with
 * different support for the list of CartItems.
 */
public abstract class BaseShoppingCartRecord {
    private String transactionId;
    private String country;
    private boolean transactionCompleted;
    private long transactionTime;
    private String customerId;
    private String paymentMethod;
    private String shippingAddress;
    private double shippingCost;
    private String couponCode;


    public BaseShoppingCartRecord() {
        setItems(new ArrayList<>());
    }

    public BaseShoppingCartRecord(BaseShoppingCartRecord clone) {
        setCustomerId(clone.getCustomerId());
        setCountry(clone.getCountry());
        setCouponCode(clone.getCouponCode());
        setItems(new ArrayList<>(clone.getItems()));
        setPaymentMethod(clone.getPaymentMethod());
        setShippingAddress(clone.getShippingAddress());
        setShippingCost(clone.getShippingCost());
        setTransactionCompleted(clone.isTransactionCompleted());
        setTransactionTime(clone.getTransactionTime());

        List<CartItem> items = getItems();
        for (int i = 0; i < items.size(); i++) {
            items.set(i, new CartItem(items.get(i)));
        }
    }

    public abstract List<CartItem> getItems();

    public abstract void setItems(List<CartItem> items);

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
        return String.format("%s at %d with %d items", transactionId, transactionTime, getItems().size());
    }
}
