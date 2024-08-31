package com.ververica.flink.training.common;

public class ProductInfoRecord {
    private final String productId;
    private final String productName;
    private final String category;
    private final double weightKg;

    // Constructor
    public ProductInfoRecord(String productId, String productName, String category, double weightKg) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.weightKg = weightKg;
    }

    // Getters
    public String getProductId() { return productId; }
    public String getProductName() { return productName; }
    public String getCategory() { return category; }
    public double getWeightKg() { return weightKg; }
}
