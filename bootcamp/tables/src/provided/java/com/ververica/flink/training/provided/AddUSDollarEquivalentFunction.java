package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.provided.ShoppingCartWithExchangeRate;
import com.ververica.flink.training.provided.TrimmedShoppingCart;
import org.apache.flink.api.common.functions.MapFunction;

public class AddUSDollarEquivalentFunction implements MapFunction<ShoppingCartWithExchangeRate, TrimmedShoppingCart> {

    @Override
    public TrimmedShoppingCart map(ShoppingCartWithExchangeRate in) throws Exception {
        TrimmedShoppingCart result = new TrimmedShoppingCart(in);

        for (CartItem item : result.getItems()) {
            item.setUsDollarEquivalent(item.getPrice() * in.getExchangeRate());
        }

        return result;
    }
}
