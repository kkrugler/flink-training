package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class CreateBatchedCarts extends RichFlatMapFunction<ECommerceRecord, Tuple2<Integer, BatchedCarts>> {

    private static final int MAX_BATCHED_RECORDS = 1000;

    private final Integer reportKey;
    private final ReportBy reportBy;

    private transient Map<String, BatchedCarts.Builder> pendingBatches;
    private transient int totalBatchedRecords;

    public CreateBatchedCarts(int reportKey, ReportBy reportBy) {
        this.reportKey = reportKey;
        this.reportBy = reportBy;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        pendingBatches = new LinkedHashMap<>(1000, 0.75f, true);
        totalBatchedRecords = 0;
    }

    @Override
    public void flatMap(ECommerceRecord in, Collector<Tuple2<Integer, BatchedCarts>> out) throws Exception {
        String newKey = reportBy.getKey(in);
        if (newKey == null) {
            for (String key : pendingBatches.keySet()) {
                out.collect(Tuple2.of(reportKey, pendingBatches.get(key).build()));
            }

            // Generate special last BatchedCart record, to trigger flush of merge-sorted
            // records downstream.
            out.collect(Tuple2.of(reportKey, new BatchedCarts()));

            return;
        }

        // Get the key from the incoming record, and see if we already have a batch for it.

        BatchedCarts.Builder builder = pendingBatches.get(newKey);
        if (builder == null) {
            builder = new BatchedCarts.Builder();
            pendingBatches.put(newKey, builder);
        }

        builder.add(in);

        totalBatchedRecords++;

        while (totalBatchedRecords > MAX_BATCHED_RECORDS) {
            String key = pendingBatches.keySet().iterator().next();
            BatchedCarts bc = pendingBatches.remove(key).build();
            out.collect(Tuple2.of(reportKey, bc));
            totalBatchedRecords -= bc.getNumCarts();
        }
    }

}
