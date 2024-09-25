package com.ververica.flink.training;

import com.ververica.flink.training.common.ShoppingCartGenerator;
import com.ververica.flink.training.common.ShoppingCartRecord;

public class ECommerceWindowingTestUtils {
    public static ShoppingCartRecord createShoppingCart(ShoppingCartGenerator generator, String country) {
        ShoppingCartRecord result = (ShoppingCartRecord)generator.createShoppingCart();
        result.setCountry(country);
        result.getItems().add(generator.createCartItem(country));
        return result;
    }

}
