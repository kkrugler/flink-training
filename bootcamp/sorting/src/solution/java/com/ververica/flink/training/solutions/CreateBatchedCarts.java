package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * For each report, create "BatchedCart" records that contain data for N incoming records,
 * by leveraging support in BatchedCart.Builder class to create optimized sets of records.
 */
public class CreateBatchedCarts extends RichFlatMapFunction<ECommerceRecord, Tuple2<Integer, BatchedCarts>> {

    private static final int MAX_BATCHED_RECORDS = 100;

    private final List<ReportBy> reports;
    private final int maxBatchedRecordsPerReport;
    private final int maxParallelism;

    // One batch per report, keyed by reportKey (0..n-1)
    private transient Map<Integer, BatchedCarts.Builder> pendingBatches;
    private transient List<Integer> reportKeys;

    public CreateBatchedCarts(List<ReportBy> reports, int maxParallelism) {
        this.reports = reports;
        this.maxBatchedRecordsPerReport = MAX_BATCHED_RECORDS / reports.size();
        this.maxParallelism = maxParallelism;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        pendingBatches = new HashMap<>();

        // reportIndex is 0...numReports-1, and we use a pre-calculated key that
        // will send report 0 to slot 0, report 1 to slot 1, and so on. This assumes one
        // slot per TM, if we have more than one slot per TM then we'd need to figure out
        // an optimal slot assignment that evenly spreads out the load.

        final int numReports = reports.size();
        reportKeys = new ArrayList<>(numReports);
        for (int reportIndex = 0; reportIndex < numReports; reportIndex++) {
            reportKeys.add(makeKeyForOperatorIndex(maxParallelism, numReports, reportIndex));
        }
    }

    @Override
    public void flatMap(ECommerceRecord in, Collector<Tuple2<Integer, BatchedCarts>> out) throws Exception {
        // For each report, process the incoming record by batching it.
        final boolean isEndRecord = in.isEndRecord();
        for (int reportIndex = 0; reportIndex < reports.size(); reportIndex++) {
            BatchedCarts.Builder builder = pendingBatches.get(reportIndex);
            final int reportKey = reportKeys.get(reportIndex);

            if (isEndRecord) {
                if (builder != null) {
                    out.collect(Tuple2.of(reportKey, builder.build()));
                    // TODO - add builder.clear(), create builders in open method,
                    // and then don't remove them here/below.
                    pendingBatches.remove(reportIndex);
                }

                // Generate special last BatchedCart record, to trigger flush of merge-sorted
                // records downstream.
                out.collect(Tuple2.of(reportKey, BatchedCarts.makeEndRecord()));
            } else {
                if (builder == null) {
                    builder = new BatchedCarts.Builder(reports.get(reportIndex));
                    pendingBatches.put(reportIndex, builder);
                }

                builder.add(in);

                if (builder.getNumCarts() >= maxBatchedRecordsPerReport) {
                    out.collect(Tuple2.of(reportKey, builder.build()));
                    pendingBatches.remove(reportIndex);
                }
            }
        }
    }

    private static Integer makeKeyForOperatorIndex(int maxParallelism, int parallelism,
                                                   int operatorIndex) {
        if (maxParallelism == ExecutionConfig.PARALLELISM_AUTO_MAX) {
            maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        }

        for (int i = 0; i < maxParallelism * 2; i++) {
            Integer key = i;
            int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
            int index = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism,
                    parallelism, keyGroup);
            if (index == operatorIndex) {
                return key;
            }
        }

        throw new RuntimeException(String.format(
                "Unable to find key for target operator index %d (max parallelism = %d, parallelism = %d",
                operatorIndex, maxParallelism, parallelism));
    }


}
