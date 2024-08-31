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
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Add a global per-5 minute window as a second result. This should generate
 * Tuple2(time, count) results that are for all countries.
 */
public class ECommerceWindowing2Workflow {

    private DataStream<ShoppingCartRecord> cartStream;
    private Sink<Tuple3<String, Long, Integer>> oneMinuteSink;
    private Sink<Tuple2<Long, Integer>> fiveMinuteSink;

    public ECommerceWindowing2Workflow() {
    }

    public ECommerceWindowing2Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public ECommerceWindowing2Workflow setOneMinuteSink(Sink<Tuple3<String, Long, Integer>> oneMinuteSink) {
        this.oneMinuteSink = oneMinuteSink;
        return this;
    }

    public ECommerceWindowing2Workflow setFiveMinuteSink(Sink<Tuple2<Long, Integer>> fiveMinuteSink) {
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
        DataStream<Tuple3<String, Long, Integer>> oneMinuteStream = filtered
                .keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountCartItemsAggregator(), new SetKeyAndTimeFunction());

        oneMinuteStream
                .sinkTo(oneMinuteSink);

        // TODO - use a global window (unkeyed), generate results for a 5 minute window.
        DataStream<Tuple2<Long, Integer>> fiveMinuteStream = null;

        fiveMinuteStream
                .sinkTo(fiveMinuteSink);

    }

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


    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Integer, Tuple3<String, Long, Integer>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Integer> elements, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
            out.collect(Tuple3.of(key, ctx.window().getStart(), elements.iterator().next()));
        }
    }

}