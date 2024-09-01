package com.ververica.flink.training.common;

public class ShoppingCartGenerator extends BaseShoppingCartGenerator<ShoppingCartRecord> {

    public ShoppingCartGenerator(long startingTime) {
        super(startingTime);
    }

    @Override
    public ShoppingCartRecord createShoppingCartRecord() {
        return new ShoppingCartRecord();
    }
}
