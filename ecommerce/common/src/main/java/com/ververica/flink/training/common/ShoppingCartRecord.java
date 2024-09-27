package com.ververica.flink.training.common;

import java.util.ArrayList;
import java.util.List;

/**
 * A typical eCommerce shopping cart.
 */
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
    private List<CartItem> items;


    public ShoppingCartRecord() {
        setItems(new ArrayList<>());
    }

    public ShoppingCartRecord(ShoppingCartRecord clone) {
        setTransactionId(clone.getTransactionId());
        setCountry(clone.getCountry());
        setTransactionCompleted(clone.isTransactionCompleted());
        setTransactionTime(clone.getTransactionTime());
        setCustomerId(clone.getCustomerId());
        setPaymentMethod(clone.getPaymentMethod());
        setShippingAddress(clone.getShippingAddress());
        setShippingCost(clone.getShippingCost());
        setCouponCode(clone.getCouponCode());

        setItems(new ArrayList<>());
        for (CartItem item : clone.getItems()) {
            getItems().add(new CartItem(item));
        }
    }

    public List<CartItem> getItems() {
        return items;
    }

    public void setItems(List<CartItem> items) {
        this.items = items;
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
