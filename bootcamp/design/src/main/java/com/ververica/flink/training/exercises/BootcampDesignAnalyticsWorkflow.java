/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.AbandonedCartItem;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * Workflow that does simple analytics, where we calculate
 * per-customer/per-hour transactions that aren't completed.
 *
 * For this exercise, add a new sink for the stream of
 * AbandonedCartItem records.
 */
public class BootcampDesignAnalyticsWorkflow {

    private DataStream<ShoppingCartRecord> cartStream;
    private Sink<KeyedWindowResult> resultSink;

    public BootcampDesignAnalyticsWorkflow() {
    }

    public BootcampDesignAnalyticsWorkflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampDesignAnalyticsWorkflow setResultSink(Sink<KeyedWindowResult> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        // Assign timestamps & watermarks. Note that we don't filter out pending
        // transactions, as those are the ones we care about.
        DataStream<ShoppingCartRecord> watermarked = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .filter(r -> r.isTransactionCompleted());

        // Key by transactionId, and filter out completed transactions. Output
        // PendingCartItem(transactionId, transactionTime, customerId, productId) records.
        // TODO - wait, is a one minute window all we need here??? Don't we want a session window, with
        // a one minute gap
        DataStream<AbandonedCartItem> uncompleted = watermarked
                .keyBy(r -> r.getTransactionId())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new FilterCompletedTransactions());

        // Key by customer id, tumbling window per hour. Return unique transactionIds per customer
        uncompleted.keyBy(r -> r.getCustomerId())
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new CountTransactionsAggregator(), new SetKeyAndTimeFunction())
                .sinkTo(resultSink);
    }

    private static class FilterCompletedTransactions extends ProcessWindowFunction<ShoppingCartRecord, AbandonedCartItem, String, TimeWindow> {

        @Override
        public void process(String transactionId, Context ctx, Iterable<ShoppingCartRecord> in, Collector<AbandonedCartItem> out) throws Exception {
            boolean foundCompleted = false;
            Set<String> productIds = new HashSet<>();
            for (ShoppingCartRecord cart : in) {
                if (cart.isTransactionCompleted()) {
                    foundCompleted = true;
                    break;
                }
            }

            if (foundCompleted) {
                return;
            }

            for (ShoppingCartRecord cart : in) {
                String customerId = cart.getCustomerId();
                long transactionTime = cart.getTransactionTime();
                for (CartItem item : cart.getItems()) {
                    out.collect(new AbandonedCartItem(transactionId, transactionTime, customerId, item.getProductId()));
                }
            }
        }
    }

    private static class CountTransactionsAggregator implements AggregateFunction<AbandonedCartItem, Set<String>, Long> {
        @Override
        public Set<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<String> add(AbandonedCartItem value, Set<String> acc) {
            acc.add(value.getTransactionId());
            return acc;
        }

        @Override
        public Long getResult(Set<String> acc) {
            return (long)acc.size();
        }

        @Override
        public Set<String> merge(Set<String> a, Set<String> b) {
            a.addAll(b);
            return a;
        }
    }

    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Long, KeyedWindowResult, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Long> elements, Collector<KeyedWindowResult> out) throws Exception {
            out.collect(new KeyedWindowResult(key, ctx.window().getStart(), elements.iterator().next()));
        }
    }
}