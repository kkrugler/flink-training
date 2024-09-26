package com.ververica.flink.training.common;

public class ECommerceTestUtils {
    public static ShoppingCartRecord createShoppingCart(ShoppingCartGenerator generator, String country) {
        ShoppingCartRecord result = (ShoppingCartRecord)generator.createShoppingCart();
        result.setCountry(country);
        result.getItems().add(generator.createCartItem(country));
        return result;
    }

}
