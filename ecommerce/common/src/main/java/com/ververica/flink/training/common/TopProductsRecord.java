package com.ververica.flink.training.common;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class TopProductsRecord {
    private long windowTime;
    // Top products, high-to-low, as (productId, count)
    private List<Tuple2<String, Integer>> topProducts;

    public TopProductsRecord() {}

    public long getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(long windowTime) {
        this.windowTime = windowTime;
    }

    public List<Tuple2<String, Integer>> getTopProducts() {
        return topProducts;
    }

    public void setTopProducts(List<Tuple2<String, Integer>> topProducts) {
        this.topProducts = topProducts;
    }
}
