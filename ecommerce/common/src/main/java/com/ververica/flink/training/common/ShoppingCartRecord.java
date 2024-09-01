package com.ververica.flink.training.common;

import java.util.List;

public class ShoppingCartRecord extends BaseShoppingCartRecord {

    private List<CartItem> items;

    public ShoppingCartRecord() {
        super();
    }

    public ShoppingCartRecord(ShoppingCartRecord clone) {
        super(clone);
    }

    public List<CartItem> getItems() {
        return items;
    }

    public void setItems(List<CartItem> items) {
        this.items = items;
    }

}
