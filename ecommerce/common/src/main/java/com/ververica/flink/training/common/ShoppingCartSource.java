package com.ververica.flink.training.common;

import java.time.Duration;

public class ShoppingCartSource extends FakeParallelSource<ShoppingCartRecord> {

    // Create a bounded source with a short delay between each record
    public ShoppingCartSource(int parallelism, long numRecords) {
        super(parallelism, numRecords, 10L, true, getShoppingCartGenerator());
    }

    // Create an unbounded source that sends records as fast as possible.
    public ShoppingCartSource(int parallelism) {
        super(parallelism, Long.MAX_VALUE, 0L, false, getShoppingCartGenerator());
    }

    private static SerializableFunction<Long, ShoppingCartRecord> getShoppingCartGenerator() {
        // Set starting time to be 10 days ago
        return new ShoppingCartGenerator(System.currentTimeMillis() - Duration.ofDays(10).toMillis());
    }

}
