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

import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.common.WindowAllResult;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class BootcampSortingSolutionWorkflow {
    private static final Logger LOGGER = LoggerFactory.getLogger(BootcampSortingSolutionWorkflow.class);

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<String> resultsSink;

    protected int maxParallelism = -1;

    public BootcampSortingSolutionWorkflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampSortingSolutionWorkflow setResultsSink(Sink<String> resultsSink) {
        this.resultsSink = resultsSink;
        return this;
    }

    public BootcampSortingSolutionWorkflow setMaxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultsSink, "resultsSink must be set");
        Preconditions.checkArgument(maxParallelism > 0, "Max parallelism must be set");

        final int reportNumber = 1;
        final int numReports = 5;

        // List<OutputTag<Tuple2<String, ShoppingCartRecord>>> tags = new ArrayList<>();

//        for (int i = 1; i <= numReports; i++) {
//            tags.add(new OutputTag<>("report-" + i));
//        }

        // Create a version of the record with a custom key that (a) has the all the
        // records going to the same slot, and (b) sorts based on the country and then
        // the customer.
        DataStream<MyKeyClass> sortable = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .map(new CreateKeyFunction(maxParallelism, reportNumber, numReports));

        sortable
                .keyBy(r -> r)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1000)))
                .process(new ConvertToText())
                .sinkTo(resultsSink);
    }

    private static class MyKeyClass implements Comparable<MyKeyClass> {
        private final ShoppingCartRecord cart;
        private final Integer partitionKey;

        public MyKeyClass(ShoppingCartRecord cart, Integer partitionKey) {
            this.cart = cart;
            this.partitionKey = partitionKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            MyKeyClass that = (MyKeyClass) o;
//            return cart.getCountry().equals(that.cart.getCountry());
            return partitionKey == that.partitionKey;
        }

        @Override
        public int hashCode() {
            // Used for partitioning, so we use the calculated value.
            return partitionKey.hashCode();
        }

        @Override
        public int compareTo(MyKeyClass o) {
            int result = Integer.compare(partitionKey, o.partitionKey);
//            if (result == 0) {
//                result = cart.getCountry().compareTo(o.cart.getCountry());
//            }
//            if (result == 0) {
//                result = cart.getPaymentMethod().compareTo(o.cart.getPaymentMethod());
//            }

            return result;
        }
    }
    private static class CreateKeyFunction extends RichMapFunction<ShoppingCartRecord, MyKeyClass> {

        private final int maxParallelism;
        private final int reportNumber;
        private final int numReports;


        public CreateKeyFunction(int maxParallelism, int reportNumber, int numReports) {
            this.maxParallelism = maxParallelism;
            this.reportNumber = reportNumber;
            this.numReports = numReports;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
        }

        @Override
        public MyKeyClass map(ShoppingCartRecord in) throws Exception {
            Integer key = makeKeyForOperatorIndex(maxParallelism, numReports, reportNumber - 1);
            return new MyKeyClass(in, key);
        }
    }

    private static class ConvertToText extends ProcessWindowFunction<MyKeyClass, String, MyKeyClass, TimeWindow> {

        @Override
        public void process(MyKeyClass key, Context ctx, Iterable<MyKeyClass> elements, Collector<String> out) throws Exception {
            System.out.println("Starting group for: " + key.cart.getCountry() + "|" + key.cart.getPaymentMethod());

            int numEntries = 0;
            for (MyKeyClass in : elements) {
                numEntries++;
                ShoppingCartRecord cart = in.cart;
                out.collect(String.format("%s\t%s\t%s\n",
                        cart.getCountry(), cart.getPaymentMethod(), cart.getTransactionId()));

            }

            System.out.println("Group size: " + numEntries);

        }

    }

    private static Integer makeKeyForOperatorIndex(int maxParallelism, int parallelism,
                                                  int operatorIndex) {
        if (maxParallelism == ExecutionConfig.PARALLELISM_AUTO_MAX) {
            maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        }

        for (int i = 0; i < maxParallelism * 2; i++) {
            Integer key = i;
            int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
            int index = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism,
                    parallelism, keyGroup);
            if (index == operatorIndex) {
                return key;
            }
        }

        throw new RuntimeException(String.format(
                "Unable to find key for target operator index %d (max parallelism = %d, parallelism = %d",
                operatorIndex, maxParallelism, parallelism));
    }

    /*
     * Return an String key that will get partitioned to the target <operatorIndex>, given the workflow's
     * <maxParallelism> (for key groups) and the operator <parallelism>.
     *
     * @param format - format for key that we'll append to (must have one %d param in it)
     * @param maxParallelism
     * @param parallelism
     * @param operatorIndex
     * @return Integer suitable for use in a record as the key.
     */
    public static String makeKeyForOperatorIndex(String format, int maxParallelism, int parallelism,
                                                 int operatorIndex) {
        if (!format.contains("%d")) {
            throw new IllegalArgumentException("Format string must contain %d");
        }

        if (maxParallelism == ExecutionConfig.PARALLELISM_AUTO_MAX) {
            maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        }

        for (int i = 0; i < maxParallelism * 2; i++) {
            String key = String.format(format, i);
            int index = getOperatorIndexForKey(key, maxParallelism, parallelism);
            if (index == operatorIndex) {
                return key;
            }
        }

        throw new RuntimeException(String.format(
                "Unable to find key for target operator index %d (max parallelism = %d, parallelism = %d",
                operatorIndex, maxParallelism, parallelism));
    }

    public static int getOperatorIndexForKey(String key, int maxParallelism, int parallelism) {
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
        return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism,
                parallelism, keyGroup);
    }

}