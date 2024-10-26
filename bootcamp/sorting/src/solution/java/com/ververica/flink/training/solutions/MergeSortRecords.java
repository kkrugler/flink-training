package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MergeSortRecords extends ProcessFunction<Tuple2<Integer, BatchedCarts>, ECommerceRecord> {

    private final ReportBy reportBy;

    private transient List<ECommerceRecord> pendingRecords;
    private transient int totalRecords;
    private transient int numBatches;

    public MergeSortRecords(ReportBy reportBy) {
        this.reportBy = reportBy;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        pendingRecords = new ArrayList<>();
    }

    @Override
    public void close() throws Exception {
        if (numBatches > 0) {
            System.out.format("Average batch: %f\n", (double) totalRecords / numBatches);
        }
    }

    @Override
    public void processElement(Tuple2<Integer, BatchedCarts> in, Context ctx, Collector<ECommerceRecord> out) throws Exception {
        if (in.f1.getKey() == null) {

            // TODO - flush merge-sorted records.
            Collections.sort(pendingRecords, reportBy);

            for (ECommerceRecord r : pendingRecords) {
                out.collect(r);
            }
        } else {
            numBatches++;
            totalRecords += in.f1.size();

            // TODO - add BatchedCarts to merge-sort dataset
            for (ECommerceRecord record : in.f1) {
                pendingRecords.add(record);
            }
        }

    }
}
