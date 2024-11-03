package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;

public abstract class SortRecordsFunction extends ProcessFunction<Tuple2<Integer, BatchedCarts>, ECommerceRecord> {
}
