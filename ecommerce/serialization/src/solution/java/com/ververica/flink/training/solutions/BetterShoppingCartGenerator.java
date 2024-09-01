package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.BaseShoppingCartGenerator;
import com.ververica.flink.training.common.ShoppingCartRecord;

public class BetterShoppingCartGenerator extends BaseShoppingCartGenerator<BetterShoppingCartRecord> {

    public BetterShoppingCartGenerator(long startingTime) {
        super(startingTime);
    }

    @Override
    public BetterShoppingCartRecord createShoppingCartRecord() {
        return new BetterShoppingCartRecord();
    }
}