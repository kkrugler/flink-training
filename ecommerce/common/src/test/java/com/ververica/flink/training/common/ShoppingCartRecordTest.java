package com.ververica.flink.training.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShoppingCartRecordTest {

    @Test
    void testFromStringNoItems() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0L);
        ShoppingCartRecord r1 = generator.createShoppingCart();
        String s1 = r1.toString();
        ShoppingCartRecord r2 = ShoppingCartRecord.fromString(s1);
        assertEquals(r1, r2);
    }
    @Test
    void testFromStringMultipleItems() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0L);
        ShoppingCartRecord r1 = generator.createShoppingCart();
        r1.getItems().add(generator.createCartItem(r1.getCountry()));
        r1.getItems().add(generator.createCartItem(r1.getCountry()));
        String s1 = r1.toString();
        ShoppingCartRecord r2 = ShoppingCartRecord.fromString(s1);
        assertEquals(r1, r2);
    }



}