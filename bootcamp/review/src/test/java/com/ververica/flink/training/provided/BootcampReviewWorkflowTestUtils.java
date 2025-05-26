/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.provided;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.exercises.BootcampReview1Workflow;
import org.assertj.core.data.Offset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BootcampReviewWorkflowTestUtils {

    public static void testReview1Workflow(BootcampReview1Workflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        TestSink sink = new TestSink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow.setCartStream(env.fromData(records).setParallelism(1)).setResultSink(sink).build();

        env.execute("BootcampReview1Workflow");

        // Do a manual calculation of the cost, mimicing what the workflow
        // would do. If we had fixed input data then we wouldn't need to worry
        // about this, but we're (somewhat lazily) re-using the fake data
        // generator, which creates random product ids.
        Map<String, Double> expected = new HashMap<>();
        for (ShoppingCartRecord r : records) {
            if (!r.isTransactionCompleted() || !r.getCountry().equals("US")) {
                continue;
            }

            double cost = 0.0;
            List<String> keys = new ArrayList<>();
            for (CartItem ci : r.getItems()) {
                keys.add(String.format("%s|%s", r.getTransactionId(), ci.getProductId()));
                cost += ci.getPrice() * ci.getQuantity();
            }

            for (String key : keys) {
                assertThat(expected.put(key, cost)).isNull();
            }
        }

        // Validate results
        for (CartItemWithShoppingCartInfo ci : sink.getSink()) {
            String key = String.format("%s|%s", ci.getTransactionId(), ci.getProductId());
            Double cost = expected.remove(key);
            assertThat(cost).isNotNull();
            assertThat(cost).isCloseTo(ci.getCost(), Offset.offset(0.01));
        }

        assertThat(expected).isEmpty();
    }

    private static class TestSink extends MockSink<CartItemWithShoppingCartInfo> {

        private static ConcurrentLinkedQueue<CartItemWithShoppingCartInfo> QUEUE =
                new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<CartItemWithShoppingCartInfo> getSink() {
            return QUEUE;
        }
    }
}
