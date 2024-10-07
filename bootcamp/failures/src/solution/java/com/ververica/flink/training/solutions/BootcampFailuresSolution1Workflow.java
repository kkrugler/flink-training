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

package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Solution to the first exercise in the eCommerce windowing lab.
 * We calculate a per-country/per-minute count of items in completed
 * shopping carts.
 */
public class ECommerceFailuresSolution1Workflow {

    private DataStream<ShoppingCartRecord> cartStream;
    private Sink<String> resultSink;

    public ECommerceFailuresSolution1Workflow() {
    }

    public ECommerceFailuresSolution1Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public ECommerceFailuresSolution1Workflow setResultSink(Sink<String> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        // Assign timestamps & watermarks, and filter out pending carts
        DataStream<ShoppingCartRecord> filtered = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .filter(r -> r.isTransactionCompleted());

        // We have a special version of the SetKeyAndTimeFunction(), which will fail on the N-th
        // call to generate results, but only once.
        FailingSetKeyAndTimeFunction setKeyAndTimeFunction = new FailingSetKeyAndTimeFunction();
        setKeyAndTimeFunction.reset();

        // Key by country, tumbling window per minute
        filtered.keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountItemsAggregator(), setKeyAndTimeFunction)
                .map(r -> r.toString())
                .sinkTo(resultSink);
    }

    private static class CountItemsAggregator implements AggregateFunction<ShoppingCartRecord, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(ShoppingCartRecord value, Integer acc) {
            for (CartItem item : value.getItems()) {
                acc += item.getQuantity();
            }

            return acc;
        }

        @Override
        public Integer getResult(Integer acc) {
            return acc;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    private static class FailingSetKeyAndTimeFunction extends ProcessWindowFunction<Integer, KeyedWindowResult, String, TimeWindow> {

        private static final AtomicBoolean FAILURE_TRIGGERED = new AtomicBoolean(false);

        public void reset() {
            FAILURE_TRIGGERED.set(false);
        }

        @Override
        public void process(String key, Context ctx, Iterable<Integer> elements, Collector<KeyedWindowResult> out) throws Exception {
            int itemCount = elements.iterator().next();
            if ((itemCount == 0) && !FAILURE_TRIGGERED.getAndSet(true)) {
                throw new IndexOutOfBoundsException("Some error in the workflow");
            }

            out.collect(new KeyedWindowResult(key, ctx.window().getStart(), itemCount));
        }
    }


}