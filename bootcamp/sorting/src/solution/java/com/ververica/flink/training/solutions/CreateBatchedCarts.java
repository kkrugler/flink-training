package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 *
 */
public class CreateBatchedCarts extends RichFlatMapFunction<ECommerceRecord, Tuple2<Integer, BatchedCarts>> {

    private static final int MAX_BATCHED_RECORDS = 1000;

    private final List<ReportBy> reports;

    // TODO - just have one batch per report, not one per key
    private transient Map<String, BatchedCarts.Builder> pendingBatches;
    private transient int totalBatchedRecords;

    public CreateBatchedCarts(List<ReportBy> reports) {
        this.reports = reports;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        pendingBatches = new LinkedHashMap<>(1000, 0.75f, true);
        totalBatchedRecords = 0;

        // TODO - track pending batched records per report
        // TODO - calc per-report limit, equal to max total / number of reports.
    }

    @Override
    public void flatMap(ECommerceRecord in, Collector<Tuple2<Integer, BatchedCarts>> out) throws Exception {
        // TODO - for each report, call method with code below to handle it.
        // reportKey is 0...n-1
        final int reportKey = 0;
        String newKey = reports.get(0).getKey(in);
        if (newKey == null) {
            pendingBatches.forEach((k, v) -> out.collect(Tuple2.of(reportKey, v.build())));
            pendingBatches.clear();

            // Generate special last BatchedCart record, to trigger flush of merge-sorted
            // records downstream.
            out.collect(Tuple2.of(reportKey, new BatchedCarts()));
            return;
        }

        // Get the key from the incoming record, and see if we already have a batch for it.
        BatchedCarts.Builder builder = pendingBatches.get(newKey);
        if (builder == null) {
            builder = new BatchedCarts.Builder(new ReportByCountrySortByShippingCost());
            pendingBatches.put(newKey, builder);
        }

        builder.add(in);

        totalBatchedRecords++;

        while (totalBatchedRecords > MAX_BATCHED_RECORDS) {
            String key = pendingBatches.keySet().iterator().next();
            BatchedCarts bc = pendingBatches.remove(key).build();
            out.collect(Tuple2.of(reportKey, bc));
            totalBatchedRecords -= bc.size();
        }
    }

}
