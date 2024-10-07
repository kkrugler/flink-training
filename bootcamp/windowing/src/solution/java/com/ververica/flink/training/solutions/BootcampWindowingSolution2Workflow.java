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
import com.ververica.flink.training.common.WindowAllResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Solution to the second exercise in the eCommerce windowing lab.
 * We add a global per-5 minute window as a second result.
 */
public class ECommerceWindowingSolution2Workflow {

    private DataStream<ShoppingCartRecord> cartStream;
    private Sink<KeyedWindowResult> oneMinuteSink;
    private Sink<WindowAllResult> fiveMinuteSink;

    public ECommerceWindowingSolution2Workflow() {
    }

    public ECommerceWindowingSolution2Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public ECommerceWindowingSolution2Workflow setOneMinuteSink(Sink<KeyedWindowResult> oneMinuteSink) {
        this.oneMinuteSink = oneMinuteSink;
        return this;
    }

    public ECommerceWindowingSolution2Workflow setFiveMinuteSink(Sink<WindowAllResult> fiveMinuteSink) {
        this.fiveMinuteSink = fiveMinuteSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(oneMinuteSink, "oneMinuteSink must be set");
        Preconditions.checkNotNull(fiveMinuteSink, "fiveMinuteSink must be set");

        // Assign timestamps & watermarks, and out pending carts
        DataStream<ShoppingCartRecord> filtered = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .filter(r -> r.isTransactionCompleted());

        // Key by country, tumbling window per minute
        DataStream<KeyedWindowResult> oneMinuteStream = filtered
                .keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountCartItemsAggregator(), new SetKeyAndTimeFunction());

        oneMinuteStream
                .sinkTo(oneMinuteSink);

        DataStream<WindowAllResult> fiveMinuteStream = oneMinuteStream
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(new OneMinuteWindowCountAggregator(), new SetTimeFunction());

        fiveMinuteStream
                .sinkTo(fiveMinuteSink);

    }

    private static class CountCartItemsAggregator implements AggregateFunction<ShoppingCartRecord, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ShoppingCartRecord value, Long acc) {
            for (CartItem item : value.getItems()) {
                acc += item.getQuantity();
            }

            return acc;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Long, KeyedWindowResult, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Long> elements, Collector<KeyedWindowResult> out) throws Exception {
            out.collect(new KeyedWindowResult(key, ctx.window().getStart(), elements.iterator().next()));
        }
    }

    // ========================================================================================
    // Classes for doing aggregation to calculate per-5 minute item counts, using the output
    // of the 1-minute aggregation as input
    // ========================================================================================

    private static class OneMinuteWindowCountAggregator implements AggregateFunction<KeyedWindowResult, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(KeyedWindowResult value, Long acc) {
            return acc + value.getResult();
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class SetTimeFunction extends ProcessAllWindowFunction<Long, WindowAllResult, TimeWindow> {

        @Override
        public void process(Context ctx, Iterable<Long> elements, Collector<WindowAllResult> out) throws Exception {
            out.collect(new WindowAllResult(ctx.window().getStart(), elements.iterator().next()));
        }
    }
}