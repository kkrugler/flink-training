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
import com.ververica.flink.training.provided.CurrencyRateAPI;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Solution to the first exercise in the eCommerce enrichment lab.
 * 1. Call API to get exchange rate - cache results
 * 2. Key by country, window per minute
 * 3. Generate per-country/per minute sales in US$
 */
public class ECommerceEnrichmentSolution3Workflow {

    private DataStream<ShoppingCartRecord> cartStream;
    private Sink<Tuple3<String, Long, Double>> resultSink;
    private long startTime = System.currentTimeMillis() - Duration.ofDays(2).toMillis();

    public ECommerceEnrichmentSolution3Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public ECommerceEnrichmentSolution3Workflow setResultSink(Sink<Tuple3<String, Long, Double>> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public ECommerceEnrichmentSolution3Workflow setStartTime(long startTime) {
        this.startTime = startTime;
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

        // Enrich by calculating US$ price to all cart items, summing total based on quantity,
        // and outputtin Tuple2<country, usdEquivalent>
        DataStream<Tuple2<String, Double>> withUSPrices = filtered
                .map(new CalcTotalUSDollarPriceFunction(startTime))
                .name("Calc US dollar price");

        // Key by country, tumbling window per minute
        withUSPrices.keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new SumDollarsAggregator(), new SetKeyAndTimeFunction())
                .sinkTo(resultSink);
    }

    /**
     * Convert a ShoppingCartRecord into <Country, total price in USD equivalent> tuple
     *     by calling the currency rate API, and summing US$ equivalents * quantity
     *     for every cart item
     */
    private static class CalcTotalUSDollarPriceFunction extends RichMapFunction<ShoppingCartRecord, Tuple2<String, Double>> {

        private long startTime;
        private transient CurrencyRateAPI api;
        private transient Map<String, Double> cachedRates;

        public CalcTotalUSDollarPriceFunction(long startTime) {
            this.startTime = startTime;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            // Since the CurrencyRateAPI isn't serializable (like many external APIs),
            // we create it in the open call.
            api = new CurrencyRateAPI(startTime);

            cachedRates = new HashMap<>();
        }

        @Override
        public Tuple2<String, Double> map(ShoppingCartRecord in) throws Exception {
            String country = in.getCountry();
            long transactionTime = in.getTransactionTime();
            String cacheKey = String.format("%s-%d", country, api.getRateTimeAsIndex(transactionTime));

            double rate;
            if (cachedRates.containsKey(cacheKey)) {
                rate = cachedRates.get(cacheKey);
            } else {
                rate = api.getRate(country, transactionTime);
                cachedRates.put(cacheKey, rate);
            }

            double usdEquivalentTotal = 0.0;

            for (CartItem item : in.getItems()) {
                double usdPrice = rate * item.getPrice();
                usdEquivalentTotal += (usdPrice * item.getQuantity());
            }

            return Tuple2.of(country, usdEquivalentTotal);
        }
    }
    private static class SumDollarsAggregator implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
        @Override
        public Double createAccumulator() {
            return 0.0;
        }

        @Override
        public Double add(Tuple2<String, Double> value, Double acc) {
            return acc + value.f1;
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

    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Double, Tuple3<String, Long, Double>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Double> elements, Collector<Tuple3<String, Long, Double>> out) throws Exception {
            out.collect(Tuple3.of(key, ctx.window().getStart(), elements.iterator().next()));
        }
    }
}