package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.CartItem;

public class CartItemWithShoppingCartInfo extends CartItem {

    private String transactionId;

    private double cost;

    public CartItemWithShoppingCartInfo() {
        super();
    }

    public CartItemWithShoppingCartInfo(CartItem clone, String transactionId, double cost) {
        super(clone);
        this.transactionId = transactionId;
        this.cost = cost;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }
}
