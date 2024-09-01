package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.FakeParallelSource;
import com.ververica.flink.training.common.SerializableFunction;
import com.ververica.flink.training.common.ShoppingCartGenerator;
import com.ververica.flink.training.common.ShoppingCartRecord;

import java.time.Duration;

public class BetterShoppingCartSource extends FakeParallelSource<BetterShoppingCartRecord> {

    // Create a bounded source with a short delay between each record
    public BetterShoppingCartSource(long numRecords) {
        super(numRecords, 10L, true, getShoppingCartGenerator());
    }

    // Create an unbounded source that sends records as fast as possible.
    public BetterShoppingCartSource() {
        super(Long.MAX_VALUE, 0L, false, getShoppingCartGenerator());
    }

    private static SerializableFunction<Long, BetterShoppingCartRecord> getShoppingCartGenerator() {
        // Set starting time to be 10 days ago
        return new BetterShoppingCartGenerator(System.currentTimeMillis() - Duration.ofDays(10).toMillis());
    }
}