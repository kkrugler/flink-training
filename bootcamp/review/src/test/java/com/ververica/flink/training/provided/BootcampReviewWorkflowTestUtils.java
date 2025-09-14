/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.provided;

import com.ververica.flink.training.exercises.BootcampReview2Workflow;
import com.ververica.flink.training.exercises.BootcampReview3Workflow;
import com.ververica.flink.training.exercises.BootcampReview4Workflow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.exercises.BootcampReview1Workflow;
import org.assertj.core.data.Offset;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BootcampReviewWorkflowTestUtils {

    public static void testReview1Workflow(BootcampReview1Workflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        Workflow1Sink sink = new Workflow1Sink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow.setCartStream(env.fromData(records).setParallelism(1)).setResultSink(sink).build();

        env.execute("BootcampReview1Workflow");

        List<ShoppingCartRecord> completed = new ArrayList<>();
        records.forEach(r -> {
            if (r.isTransactionCompleted()) completed.add(r);
        });

        assertThat(sink.getSink()).containsExactlyInAnyOrderElementsOf(completed);
    }

    public static void testReview2Workflow(BootcampReview2Workflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        Workflow1Sink sink = new Workflow1Sink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow.setCartStream(env.fromData(records).setParallelism(1)).setResultSink(sink).build();

        env.execute("BootcampReview1Workflow");

        List<ShoppingCartRecord> completed = new ArrayList<>();
        records.forEach(r -> {
            if (r.isTransactionCompleted() && (r.getCountry().equals("US"))) completed.add(r);
        });

        assertThat(sink.getSink()).containsExactlyInAnyOrderElementsOf(completed);
    }


    public static void testReview3Workflow(BootcampReview3Workflow workflow) throws Exception {

        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        Workflow3Sink sink = new Workflow3Sink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow.setCartStream(env.fromData(records).setParallelism(1)).setResultSink(sink).build();

        env.execute("BootcampReview2Workflow");

        // Do a manual calculation of the cost, mimicking what the workflow
        // would do. If we had fixed input data then we wouldn't need to worry
        // about this, but we're (somewhat lazily) re-using the fake data
        // generator, which creates random product ids.
        List<ShoppingCartWithCost> expected = new ArrayList<>();
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

            expected.add(new ShoppingCartWithCost(r, cost));
        }

        assertThat(sink.getSink()).containsExactlyInAnyOrderElementsOf(expected);
    }

    public static void testReview4Workflow(BootcampReview4Workflow workflow) throws Exception {

        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        Workflow4Sink sink = new Workflow4Sink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow.setCartStream(env.fromData(records).setParallelism(1)).setResultSink(sink).build();

        env.execute("BootcampReview4Workflow");

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

    private static class Workflow1Sink extends MockSink<ShoppingCartRecord> {

        private static ConcurrentLinkedQueue<ShoppingCartRecord> QUEUE =
                new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<ShoppingCartRecord> getSink() {
            return QUEUE;
        }
    }

    private static class Workflow3Sink extends MockSink<ShoppingCartWithCost> {

        private static ConcurrentLinkedQueue<ShoppingCartWithCost> QUEUE =
                new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<ShoppingCartWithCost> getSink() {
            return QUEUE;
        }
    }

    private static class Workflow4Sink extends MockSink<CartItemWithShoppingCartInfo> {

        private static ConcurrentLinkedQueue<CartItemWithShoppingCartInfo> QUEUE =
                new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<CartItemWithShoppingCartInfo> getSink() {
            return QUEUE;
        }
    }
}
