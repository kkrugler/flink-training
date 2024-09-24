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
import com.ververica.flink.training.common.ProductInfoRecord;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.CurrencyRateAPI;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Solution to the first exercise in the eCommerce enrichment lab.
 * 1. Join the product info stream with the eCommerce records
 * 2. Call API to get exchange rate
 * 3. Key by store, window per minute
 * 4. Generate per-store/per-minute sales in US$
 */
public class ECommerceEnrichmentSolution1Workflow {

    private DataStream<ShoppingCartRecord> cartStream;
    private DataStream<ProductInfoRecord> productInfoStream;
    private Sink<Tuple3<String, Long, Integer>> resultSink;

    public ECommerceEnrichmentSolution1Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public ECommerceEnrichmentSolution1Workflow setProductInfoStream(DataStream<ProductInfoRecord> productInfoStream) {
        this.productInfoStream = productInfoStream;
        return this;
    }

    public ECommerceEnrichmentSolution1Workflow setResultSink(Sink<Tuple3<String, Long, Integer>> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(productInfoStream, "productInfoStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        // Assign timestamps & watermarks, and filter out pending carts
        DataStream<ShoppingCartRecord> filtered = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .filter(r -> r.isTransactionCompleted());

        // Enrich by adding US$ price
        DataStream<ShoppingCartRecord> withUSPrices = cartStream
                .map(new AddUSDollarPriceFunction())
                .name("Add US dollar price");

        // Key by country, tumbling window per minute
        filtered.keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountItemsAggregator(), new SetKeyAndTimeFunction())
                .sinkTo(resultSink);
    }

    private static class AddUSDollarPriceFunction extends RichMapFunction<ShoppingCartRecord, ShoppingCartRecord> {

        private transient CurrencyRateAPI api;

        @Override
        public void open(OpenContext openContext) throws Exception {
            api = new CurrencyRateAPI();
        }

        @Override
        public ShoppingCartRecord map(ShoppingCartRecord in) throws Exception {
            String country = in.getCountry();

            for (CartItem item : in.getItems()) {
                double usdPrice = api.getRate(country, in.getTransactionTime()) * item.getPrice();
                item.setUsDollarEquivalent(usdPrice);
            }

            return in;
        }

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

    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Integer, Tuple3<String, Long, Integer>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Integer> elements, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
            out.collect(Tuple3.of(key, ctx.window().getStart(), elements.iterator().next()));
        }
    }
}