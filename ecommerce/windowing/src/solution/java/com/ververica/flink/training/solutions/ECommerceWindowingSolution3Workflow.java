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
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Solution to the third exercise in the eCommerce windowing lab.
 * We add a configurable window with the top 2 longest transactions
 * as a new result. By "longest transaction" we mean a transaction's
 * duration, calculated as delta from the completed record to the
 * first record. This result is a Tuple3(transactionId, time, duration)
 */
public class ECommerceWindowingSolution3Workflow {

    private DataStream<ShoppingCartRecord> cartStream;
    private Sink<Tuple3<String, Long, Integer>> oneMinuteSink;
    private Sink<Tuple2<Long, Integer>> fiveMinuteSink;
    private Sink<Tuple3<String, Long, Long>> longestTransactionsSink;
    private int transactionWindowInMinutes = 5;

    public ECommerceWindowingSolution3Workflow() {
    }

    public ECommerceWindowingSolution3Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public ECommerceWindowingSolution3Workflow setOneMinuteSink(Sink<Tuple3<String, Long, Integer>> oneMinuteSink) {
        this.oneMinuteSink = oneMinuteSink;
        return this;
    }

    public ECommerceWindowingSolution3Workflow setFiveMinuteSink(Sink<Tuple2<Long, Integer>> fiveMinuteSink) {
        this.fiveMinuteSink = fiveMinuteSink;
        return this;
    }

    public ECommerceWindowingSolution3Workflow setLongestTransactionsSink(Sink<Tuple3<String, Long, Long>> longestTransactionsSink) {
        this.longestTransactionsSink = longestTransactionsSink;
        return this;
    }

    public ECommerceWindowingSolution3Workflow setTransactionsWindowInMinutes(int transactionWindowInMinutes) {
        this.transactionWindowInMinutes = transactionWindowInMinutes;
        return this;
    }
    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(oneMinuteSink, "oneMinuteSink must be set");
        Preconditions.checkNotNull(fiveMinuteSink, "fiveMinuteSink must be set");
        Preconditions.checkNotNull(longestTransactionsSink, "longestTransactionsSink must be set");

        // Assign timestamps & watermarks, and
        DataStream<ShoppingCartRecord> watermarkedStream = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()));

        // Filter out pending carts, key by country, tumbling window per minute
        DataStream<Tuple3<String, Long, Integer>> oneMinuteStream = watermarkedStream
                .filter(r -> r.isTransactionCompleted())
                .keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountCartItemsAggregator(), new SetKeyAndTimeFunction());

        oneMinuteStream
                .sinkTo(oneMinuteSink);

        DataStream<Tuple2<Long, Integer>> fiveMinuteStream = oneMinuteStream
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(new CountTupleItemsAggregator(), new SetTimeFunction());

        fiveMinuteStream
                .sinkTo(fiveMinuteSink);

        watermarkedStream
                // Key by transaction id, window by transaction (session) and calculate duration.
                // Generate result as Tuple3<transaction id, time, duration>
                .keyBy(r -> r.getTransactionId())
                .window(EventTimeSessionWindows.withGap(Duration.ofMillis(1)))
                .aggregate(new FindTransactionBoundsFunction(), new SetDurationAndTimeFunction())

                // Key by transaction id, window by configurable time, aggregate using PriorityQueue
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(transactionWindowInMinutes)))
                .aggregate(new FindLongestTransactions(), new EmitLongestTransactions())
                .sinkTo(longestTransactionsSink);

    }

    // ========================================================================================
    // Classes for doing aggregation to calculate per-1 minute item counts.
    // ========================================================================================

    private static class CountCartItemsAggregator implements AggregateFunction<ShoppingCartRecord, Integer, Integer> {
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

    private static class SetTimeFunction extends ProcessAllWindowFunction<Integer, Tuple2<Long, Integer>, TimeWindow> {

        @Override
        public void process(Context ctx, Iterable<Integer> elements, Collector<Tuple2<Long, Integer>> out) throws Exception {
            out.collect(Tuple2.of(ctx.window().getStart(), elements.iterator().next()));
        }
    }

    // ========================================================================================
    // Classes for doing aggregation to calculate per-5 minute item counts, using the output
    // of the 1-minute aggregation as input
    // ========================================================================================

    private static class CountTupleItemsAggregator implements AggregateFunction<Tuple3<String, Long, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple3<String, Long, Integer> value, Integer acc) {
            return acc + value.f2;
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

    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Integer, Tuple3<String, Long, Integer>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Integer> elements, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
            out.collect(Tuple3.of(key, ctx.window().getStart(), elements.iterator().next()));
        }
    }

    // ========================================================================================
    // Classes for doing aggregation to calculate shopping cart transaction durations
    // ========================================================================================

    private class FindTransactionBoundsFunction implements AggregateFunction<ShoppingCartRecord, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(-1L, -1L);
        }

        @Override
        public Tuple2<Long, Long> add(ShoppingCartRecord value, Tuple2<Long, Long> acc) {
            long transactionTime = value.getTransactionTime();
            if (value.isTransactionCompleted()) {
                acc.f1 = transactionTime;
            } else if ((acc.f0 == -1) || (acc.f0 > transactionTime)) {
                acc.f0 = transactionTime;
            }

            return acc;
        }

        @Override
        public Tuple2<Long, Long> getResult(Tuple2<Long, Long> acc) {
            return acc;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            if (a.f0 == -1) {
                a.f0 = b.f0;
            } else if ((b.f0 != -1) && (a.f0 > b.f0)) {
                a.f0 = b.f0;
            }

            if (a.f1 == -1) {
                a.f1 = b.f1;
            } else if ((b.f1 != -1) && (a.f1 > b.f1)) {
                a.f1 = b.f1;
            }

            return a;
        }
    }

    private class SetDurationAndTimeFunction extends ProcessWindowFunction<Tuple2<Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Tuple2<Long, Long>> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            Tuple2<Long, Long> interval = elements.iterator().next();

            // If we didn't get a start (probably because the session window timed out, so all we got is
            // an end) or we never got an end (cart was abandoned) then just ignore this.
            if ((interval.f0 == -1) || (interval.f1 == -1)) {
                return;
            }

            out.collect(Tuple3.of(key, ctx.window().getStart(), interval.f1 - interval.f0));
        }
    }

    // ========================================================================================
    // Classes for doing aggregation to find the N longest transactions
    // ========================================================================================

    private class FindLongestTransactions implements AggregateFunction<Tuple3<String, Long, Long>,
            PriorityQueue<Tuple3<String, Long, Long>>, List<Tuple3<String, Long, Long>>> {
        @Override
        public PriorityQueue<Tuple3<String, Long, Long>> createAccumulator() {
            return new PriorityQueue<>(new TransactionDurationComparator());
        }

        @Override
        public PriorityQueue<Tuple3<String, Long, Long>> add(Tuple3<String, Long, Long> value, PriorityQueue<Tuple3<String, Long, Long>> acc) {
            acc.add(value);
            return acc;
        }

        @Override
        public List<Tuple3<String, Long, Long>> getResult(PriorityQueue<Tuple3<String, Long, Long>> acc) {
            List<Tuple3<String, Long, Long>> result = new ArrayList<>();
            int numToReturn = Math.min(acc.size(), 2);
            for (int i = 0; i < numToReturn; i++) {
                result.add(acc.remove());
            }

            return result;
        }

        @Override
        public PriorityQueue<Tuple3<String, Long, Long>> merge(PriorityQueue<Tuple3<String, Long, Long>> a, PriorityQueue<Tuple3<String, Long, Long>> b) {
            a.addAll(b);
            return a;
        }
    }

    private static class TransactionDurationComparator
            implements Comparator<Tuple3<String, Long, Long>> {
        @Override
        public int compare(Tuple3<String, Long, Long> o1, Tuple3<String, Long, Long> o2) {
            // Return inverse sort order, longest first
            return Long.compare(o2.f2, o1.f2);
        }
    }

    private class EmitLongestTransactions extends ProcessWindowFunction<List<Tuple3<String, Long, Long>>,
            Tuple3<String, Long, Long>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<List<Tuple3<String, Long, Long>>> elements,
                            Collector<Tuple3<String, Long, Long>> out) throws Exception {
            for (Tuple3<String, Long, Long> e : elements.iterator().next()) {
                out.collect(e);
            }
        }
    }
}