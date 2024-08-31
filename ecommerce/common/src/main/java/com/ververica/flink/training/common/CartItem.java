package com.ververica.flink.training.common;

public class CartItem {
    private String productId;
    private int quantity;
    private double price;
    private double usDollarEquivalent;

    // New fields
    private String productName;
    private String category;
    private double weightKg;

    public CartItem() { }

    public CartItem(CartItem clone) {
        productId = clone.productId;
        quantity = clone.quantity;
        price = clone.price;
        usDollarEquivalent = clone.usDollarEquivalent;
        productName = clone.productName;
        category = clone.category;
        weightKg = clone.weightKg;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public double getUsDollarEquivalent() {
        return usDollarEquivalent;
    }

    public void setUsDollarEquivalent(double usDollarEquivalent) {
        this.usDollarEquivalent = usDollarEquivalent;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public double getWeightKg() {
        return weightKg;
    }

    public void setWeightKg(double weightKg) {
        this.weightKg = weightKg;
    }
}
