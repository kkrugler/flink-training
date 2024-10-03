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
import com.ververica.flink.training.common.ProductRecord;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.KeyedWindowDouble;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Solution to the second exercise in the eCommerce enrichment lab.
 * 1. Convert shopping cart records into separate ProductRecords
 * 2. Join the product info stream with the product stream
 * 3. Add product information to the product records
 * 2. Key by country, window per minute
 * 3. Generate per-product/per-minute shipping weight
 */
public class ECommerceEnrichmentSolution2Workflow {

    private DataStream<ShoppingCartRecord> cartStream;
    private DataStream<ProductInfoRecord> productInfoStream;
    private Sink<KeyedWindowDouble> resultSink;

    public ECommerceEnrichmentSolution2Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public ECommerceEnrichmentSolution2Workflow setProductInfoStream(DataStream<ProductInfoRecord> productInfoStream) {
        this.productInfoStream = productInfoStream;
        return this;
    }

    public ECommerceEnrichmentSolution2Workflow setResultSink(Sink<KeyedWindowDouble> resultSink) {
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

        // Turn into a per-product stream
        DataStream<ProductRecord> productStream = filtered
                .flatMap(new ExplodeShoppingCartFunction())
                .name("Explode shopping cart");

        // Assign timestamps & watermarks
        DataStream<ProductInfoRecord> watermarkedProduct = productInfoStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductInfoRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getInfoTime()));

        // Connect products with the product info stream, using product ID,
        // and enrich the product records.
        DataStream<ProductRecord> enrichedStream = productStream
                .keyBy(r -> r.getProductId())
                .connect(watermarkedProduct.keyBy(r -> r.getProductId()))
                .process(new AddProductInfoFunction())
                .name("Enrich products");

        // Key by country, tumbling window per minute
        enrichedStream.keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new SumWeightAggregator(), new SetKeyAndTimeFunction())
                .sinkTo(resultSink);
    }

    private static class ExplodeShoppingCartFunction implements FlatMapFunction<ShoppingCartRecord, ProductRecord> {

        @Override
        public void flatMap(ShoppingCartRecord in, Collector<ProductRecord> out) throws Exception {
            for (CartItem item : in.getItems()) {
                out.collect(new ProductRecord(in, item));
            }
        }
    }

    private static class AddProductInfoFunction extends KeyedCoProcessFunction<String, ProductRecord, ProductInfoRecord, ProductRecord> {

        private transient ListState<ProductRecord> pendingLeft;
        private transient ValueState<ProductInfoRecord> pendingRight;

        @Override
        public void open(OpenContext ctx) throws Exception {
            pendingLeft = getRuntimeContext().getListState(new ListStateDescriptor<>("left", ProductRecord.class));
            pendingRight = getRuntimeContext().getState(new ValueStateDescriptor<>("right", ProductInfoRecord.class));
        }

        @Override
        public void processElement1(ProductRecord left, Context ctx, Collector<ProductRecord> out) throws Exception {
            ProductInfoRecord right = pendingRight.value();
            if (right != null) {
                left.setCategory(right.getCategory());
                left.setWeightKg(right.getWeightKg());
                left.setProductName(right.getProductName());
                out.collect(left);
            } else {
                // We need to wait for the product info record to arrive.
                pendingLeft.add(left);
            }
        }

        @Override
        public void processElement2(ProductInfoRecord right, Context ctx, Collector<ProductRecord> out) throws Exception {
            pendingRight.update(right);

            // If there are any pending left-side records, output the enriched version now.
            for (ProductRecord left : pendingLeft.get()) {
                left.setCategory(right.getCategory());
                left.setWeightKg(right.getWeightKg());
                left.setProductName(right.getProductName());
                out.collect(left);
            }

            pendingLeft.clear();
        }
    }

    private static class SumWeightAggregator implements AggregateFunction<ProductRecord, Double, Double> {
        @Override
        public Double createAccumulator() {
            return 0.0;
        }

        @Override
        public Double add(ProductRecord value, Double acc) {
            return acc + (value.getWeightKg() * value.getQuantity());
        }

        @Override
        public Double getResult(Double acc) {
            return acc;
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Double, KeyedWindowDouble, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Double> elements, Collector<KeyedWindowDouble> out) throws Exception {
            out.collect(new KeyedWindowDouble(key, ctx.window().getStart(), elements.iterator().next()));
        }
    }
}