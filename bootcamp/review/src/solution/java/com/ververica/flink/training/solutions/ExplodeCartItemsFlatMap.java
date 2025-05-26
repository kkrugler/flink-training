package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.provided.CartItemWithShoppingCartInfo;
import com.ververica.flink.training.provided.ShoppingCartWithCost;

public class ExplodeCartItemsFlatMap
        implements FlatMapFunction<ShoppingCartWithCost, CartItemWithShoppingCartInfo> {

    @Override
    public void flatMap(ShoppingCartWithCost in, Collector<CartItemWithShoppingCartInfo> out)
            throws Exception {
        for (CartItem item : in.getItems()) {
            out.collect(
                    new CartItemWithShoppingCartInfo(item, in.getTransactionId(), in.getCost()));
        }
    }
}
