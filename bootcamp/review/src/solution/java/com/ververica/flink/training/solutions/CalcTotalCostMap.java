package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.functions.MapFunction;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.ShoppingCartWithCost;

public class CalcTotalCostMap implements MapFunction<ShoppingCartRecord, ShoppingCartWithCost> {

    @Override
    public ShoppingCartWithCost map(ShoppingCartRecord value) throws Exception {
        double totalCost = 0;
        for (CartItem item : value.getItems()) {
            totalCost += (item.getPrice() * item.getQuantity());
        }

        ShoppingCartWithCost result = new ShoppingCartWithCost(value, totalCost);
        return result;
    }
}
