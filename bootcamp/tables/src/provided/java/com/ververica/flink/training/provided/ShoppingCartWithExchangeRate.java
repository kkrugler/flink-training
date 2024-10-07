package com.ververica.flink.training.provided;

public class ShoppingCartWithExchangeRate extends TrimmedShoppingCart {

    private double exchangeRate;

    public ShoppingCartWithExchangeRate() { }

    public double getExchangeRate() {
        return exchangeRate;
    }

    public void setExchangeRate(double exchangeRate) {
        this.exchangeRate = exchangeRate;
    }
}
