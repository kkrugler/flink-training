package com.ververica.flink.training.common;

public class ProductInfoRecord {
    private String productId;
    private String productName;
    private String category;
    private double weightKg;

    public ProductInfoRecord() {}

    // Constructor
    public ProductInfoRecord(String productId, String productName, String category, double weightKg) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.weightKg = weightKg;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
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
