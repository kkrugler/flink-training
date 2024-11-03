package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MergeSortRecordsTest {

    @Test
    public void testMergeSort() throws Exception {
        ReportBy reportBy = new ReportByCountrySortByShippingCost();
        List<ReportBy> reports = new ArrayList<>();
        reports.add(reportBy);

        final int upstreamParallelism = 2;
        MergeSortRecords processFunction = new MergeSortRecords(reports, upstreamParallelism);
        OneInputStreamOperatorTestHarness<Tuple2<Integer, BatchedCarts>, ECommerceRecord> testHarness =
                new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(processFunction));

        testSortFunction(testHarness);
    }

    public static void testSortFunction(OneInputStreamOperatorTestHarness<Tuple2<Integer, BatchedCarts>,
            ECommerceRecord> testHarness) throws Exception {
        ReportBy reportBy = new ReportByCountrySortByShippingCost();
        List<ReportBy> reports = new ArrayList<>();
        reports.add(reportBy);

        final int upstreamParallelism = 2;

        testHarness.open();

        ECommerceRecord r1 = new ECommerceRecord();
        r1.setCountry("US");
        r1.setCouponCode("");
        r1.setCustomerId("C1");
        r1.setPaymentMethod("PayPal");
        r1.setShippingCost(10.0);
        r1.setTransactionId("T1");
        r1.setShippingAddress("Shipping address");
        r1.setTransactionTime(0);

        ECommerceRecord r2 = new ECommerceRecord();
        r2.setCountry("US");
        r2.setCouponCode("");
        r2.setCustomerId("C2");
        r2.setPaymentMethod("PayPal");
        r2.setShippingCost(5.0);
        r2.setTransactionId("T2");
        r2.setShippingAddress("Shipping address");
        r2.setTransactionTime(0);

        BatchedCarts.Builder builder = new BatchedCarts.Builder(reportBy);
        builder.add(r1);
        builder.add(r2);

        testHarness.processElement(Tuple2.of(6, builder.build()), 0L);
        assertTrue(testHarness.getOutput().isEmpty());

        // Because upstreamParallelism is 2, we need two records to trigger our
        // batch to be flushed.
        testHarness.processElement(Tuple2.of(6, BatchedCarts.makeEndRecord()), 0L);
        assertTrue(testHarness.getOutput().isEmpty());
        testHarness.processElement(Tuple2.of(6, BatchedCarts.makeEndRecord()), 0L);

        // Filter out "keep-alive" records generated while we wait for the merge-sort
        // to complete.
        List<ECommerceRecord> filteredResults = new ArrayList<>();
        for (StreamRecord<ECommerceRecord> sr : testHarness.getRecordOutput()) {
            ECommerceRecord r = sr.getValue();
            if (!r.isEndRecord()) {
                filteredResults.add(r);
            }
        }

        assertThat(filteredResults).containsExactly(
                r2,
                r1
        );

        testHarness.close();
    }
}