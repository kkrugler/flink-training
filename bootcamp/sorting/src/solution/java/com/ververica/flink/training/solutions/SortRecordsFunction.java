package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/**
 * Base class for the in-memory and merge-sort implementations of this ProcessFunction.
 */
public abstract class SortRecordsFunction extends ProcessFunction<Tuple2<Integer, BatchedCarts>, ECommerceRecord> {
}
