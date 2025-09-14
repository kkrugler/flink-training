package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.ShoppingCartRecord;

public class ShoppingCartWithCost extends ShoppingCartRecord {

    private double cost;

    public ShoppingCartWithCost() {
        super();
    }

    public ShoppingCartWithCost(ShoppingCartRecord clone, double cost) {
        super(clone);
        setCost(cost);
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }
}
