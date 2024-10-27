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

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.*;

public class BootcampSortingSolutionJob {

    public static void main(String[] args) throws Exception {
        final boolean discarding = true;
        final int numReports = 5;
        final long numRecords = 1_000_000;
        final int maxParallelism = 400;

        ParameterTool parameters = ParameterTool.fromArgs(args);
//        Configuration config = new Configuration();
//        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredLocalEnvironment(parameters);
        env.setMaxParallelism(maxParallelism);

        ShoppingCartSource realSource = new ShoppingCartSource(numRecords, 0L);
        FakeParallelSource<ShoppingCartRecord> endSource = new FakeParallelSource<ShoppingCartRecord>(env.getParallelism(), 0, true, new EndRecordGenerator());

        HybridSource<ShoppingCartRecord> realPlusEnd = HybridSource.builder(realSource)
                .addSource(endSource)
                .build();

        DataStream<ECommerceRecord> records = env.fromSource(realPlusEnd,
                        WatermarkStrategy.noWatermarks(), "Shopping Cart Stream", TypeInformation.of(ShoppingCartRecord.class))
                .map(new EnrichWithShippingCost())
                .map(r -> new ECommerceRecord(r));

        new BootcampSortingSolutionWorkflow()
                .setCartStream(records)
                // TODO - support writing to a FileSink
                .setResultsSink(discarding ? new DiscardingSink<>() : new PrintSink<>())
                .setMaxParallelism(maxParallelism)
                .addReport(new ReportByCountrySortByShippingCost())
                // TODO - add another report, maybe per customer by shipping cost?
                .build();

        // Verify no acc is expected.
        JobExecutionResult jobResult = env.execute("BootcampSortingSolutionJob");
        Map<String, Object> acc = jobResult.getAllAccumulatorResults();
        for (String accKey : acc.keySet()) {
            System.out.format("%s: %s\n", accKey, acc.get(accKey));
        }
    }

    private static class EndRecordGenerator implements SerializableFunction<Long, ShoppingCartRecord> {

        @Override
        public ShoppingCartRecord apply(Long aLong) {
            ShoppingCartRecord result = new ShoppingCartRecord();
            result.setCountry(null);
            result.setPaymentMethod(null);
            return result;
        }
    }

    private static class EnrichWithShippingCost extends RichMapFunction<ShoppingCartRecord, ShoppingCartRecord> {

        private transient Map<String, ProductInfoRecord> products;

        @Override
        public ShoppingCartRecord map(ShoppingCartRecord value) throws Exception {
            double totalWeight = 0.0;
            for (CartItem item : value.getItems()) {
                ProductInfoRecord pir = products.get(item.getProductId());
                if (pir == null) {
                    throw new NoSuchElementException();
                }

                totalWeight += (pir.getWeightKg() * item.getQuantity());
            }

            // Cost is weight * costPerKg
            value.setShippingCost(totalWeight * 0.73);
            return value;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            products = new HashMap<>();

            ProductInfoGenerator productGenerator = new ProductInfoGenerator();
            for (long i = 0; i < ProductInfoGenerator.NUM_UNIQUE_PRODUCTS; i++) {
                ProductInfoRecord pir = productGenerator.apply(i);
                products.put(pir.getProductId(), pir);
            }
        }
    }

}