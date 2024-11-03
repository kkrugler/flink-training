package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MemorySortRecordsTest {

    @Test
    public void testMemorySort() throws Exception {
        ReportBy reportBy = new ReportByCountrySortByShippingCost();
        List<ReportBy> reports = new ArrayList<>();
        reports.add(reportBy);

        final int upstreamParallelism = 2;
        MemorySortRecords processFunction = new MemorySortRecords(reports, upstreamParallelism);
        OneInputStreamOperatorTestHarness<Tuple2<Integer, BatchedCarts>, ECommerceRecord> testHarness =
                new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(processFunction));

        MergeSortRecordsTest.testSortFunction(testHarness);
    }

}