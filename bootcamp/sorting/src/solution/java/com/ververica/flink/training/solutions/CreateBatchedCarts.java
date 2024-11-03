package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * For each report, create "BatchedCart" records that contain data for N incoming records,
 * by leveraging support in BatchedCart.Builder class to create optimized sets of records.
 */
public class CreateBatchedCarts extends RichFlatMapFunction<ECommerceRecord, Tuple2<Integer, BatchedCarts>> {

    private static final int MAX_BATCHED_RECORDS = 10_000;

    private final List<ReportBy> reports;
    private final int maxBatchedRecordsPerReport;

    // One batch per report, keyed by reportKey (0..n-1)
    private transient Map<Integer, BatchedCarts.Builder> pendingBatches;

    public CreateBatchedCarts(List<ReportBy> reports) {
        this.reports = reports;
        this.maxBatchedRecordsPerReport = MAX_BATCHED_RECORDS / reports.size();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        pendingBatches = new HashMap<>();
    }

    @Override
    public void flatMap(ECommerceRecord in, Collector<Tuple2<Integer, BatchedCarts>> out) throws Exception {
        // TODO - for each report, call method with code below to handle it.
        // reportNumber is 0...numReports-1, and we need to use the pre-calculated key that
        // will send report 0 to slot 0, report 1 to slot 1, and so on. This assumes one
        // slot per TM, if we have more than one slot per TM then we'd need to figure out
        // an optimal slot assignment that evenly spreads out the load.
        final int reportKey = 0;
        if (in.isEndRecord()) {
            pendingBatches.forEach((k, v) -> out.collect(Tuple2.of(reportKey, v.build())));
            pendingBatches.clear();

            // Generate special last BatchedCart record, to trigger flush of merge-sorted
            // records downstream.
            out.collect(Tuple2.of(reportKey, BatchedCarts.makeEndRecord()));
            return;
        }

        // Get the key from the incoming record, and see if we already have a batch for it.
        BatchedCarts.Builder builder = pendingBatches.get(reportKey);
        if (builder == null) {
            builder = new BatchedCarts.Builder(new ReportByCountrySortByShippingCost());
            pendingBatches.put(reportKey, builder);
        }

        builder.add(in);

        if (builder.getNumCarts() >= maxBatchedRecordsPerReport) {
            BatchedCarts bc = builder.build();
            out.collect(Tuple2.of(reportKey, bc));
            pendingBatches.remove(reportKey);
        }
    }

}
