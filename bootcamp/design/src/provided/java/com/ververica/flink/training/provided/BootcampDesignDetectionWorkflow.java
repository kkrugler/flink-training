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

package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.common.WindowAllResult;
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

/**
 * Given a stream of AbandonedCartItem records, identify customers who seem to be
 * engaged in product-boosting behavior, by adding a lot of products to carts
 * that are never completed (purchased).
 */
public class BootcampDesignDetectionWorkflow {

    private static final long DEFAULT_ABANDONED_LIMIT = 20;

    private DataStream<AbandonedCartItem> abandonedStream;
    private Sink<KeyedWindowResult> resultSink;
    private long maxAbandonedPerHour = DEFAULT_ABANDONED_LIMIT;

    public BootcampDesignDetectionWorkflow() {
    }

    public BootcampDesignDetectionWorkflow setAbandonedStream(DataStream<AbandonedCartItem> abandonedStream) {
        this.abandonedStream = abandonedStream;
        return this;
    }

    public BootcampDesignDetectionWorkflow setResultSink(Sink<KeyedWindowResult> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public BootcampDesignDetectionWorkflow setMaxAbandonedPerHour(long maxAbandonedPerHour) {
        this.maxAbandonedPerHour = maxAbandonedPerHour;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(abandonedStream, "abandonedStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        // Key by customer, tumbling window per hour.
        // Count number of transactions, then filter to only the suspected items.
        abandonedStream.keyBy(r -> r.getCustomerId())
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new CountTransactionsAggregator(), new SetKeyAndTimeFunction())
                .filter(r -> r.getResult() >= maxAbandonedPerHour)
                .sinkTo(resultSink);
    }

    private static class CountTransactionsAggregator implements AggregateFunction<AbandonedCartItem, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AbandonedCartItem value, Long acc) {
            return acc + 1;
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
}