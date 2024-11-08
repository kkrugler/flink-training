package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import com.ververica.flink.training.solutions.inmemory.MemorySortRecords;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class MemorySortRecordsTest {

    @Test
    public void testMemorySort() throws Exception {
        List<ReportBy> reports = new ArrayList<>();
        reports.add(new ReportByCountrySortByShippingCost());
        reports.add(new ReportByCustomerIdSortByTransactionTime());

        final int upstreamParallelism = 2;
        MemorySortRecords processFunction = new MemorySortRecords(reports, upstreamParallelism);
        OneInputStreamOperatorTestHarness<Tuple2<Integer, BatchedCarts>, ECommerceRecord> testHarness =
                new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(processFunction));

        MergeSortRecordsTest.testSortByCountryFunction(testHarness, upstreamParallelism);
    }

}