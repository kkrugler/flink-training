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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import com.ververica.flink.training.solutions.ECommerceRecord;

/**
 * We want to take a stream of ShoppingCartRecords, and output
 * them  as a TSV (tab separated value) text file, in sorted order.
 * This means a global sort, and a single writer, for a batch
 * Flink job. Normally this would be limited by the amount of
 * memory available to do an in-memory sort, and constrained by
 * the performance of a single CPU which is handling all of the
 * data, but we'll illustrate several optimizations.
 */
public class BootcampSortingSolutionWorkflow {
    private static final Logger LOGGER = LoggerFactory.getLogger(BootcampSortingSolutionWorkflow.class);

    protected DataStream<ECommerceRecord> cartStream;
    protected Sink<String> resultsSink;

    protected int maxParallelism = -1;

    public BootcampSortingSolutionWorkflow setCartStream(DataStream<ECommerceRecord> cartStream) {
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

        // Do a map-side pre-sort, where we group records into "batches" that all
        // share the same sorting key.
        DataStream<Tuple2<String, BatchedCarts>> batched = cartStream
                .flatMap(new CreateBatchedCarts(maxParallelism, reportNumber, numReports));

        batched
                .partitionCustom(r -> r.get)
                .keyBy(t -> t.f0)
                .process(new ConvertToText())
                .sinkTo(resultsSink);
    }

    private static class CreateBatchedCarts extends RichFlatMapFunction<ECommerceRecord, Tuple2<String, BatchedCarts>> {

        private final int maxParallelism;
        private final int reportNumber;
        private final int numReports;

        private transient Map<String, BatchedCarts.Builder> pendingBatches;
        private transient int totalBatchedRecords;

        public CreateBatchedCarts(int maxParallelism, int reportNumber, int numReports) {
            this.maxParallelism = maxParallelism;
            this.reportNumber = reportNumber;
            this.numReports = numReports;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            pendingBatches = new HashMap<>();
            totalBatchedRecords = 0;
        }

        @Override
        public void flatMap(ECommerceRecord in, Collector<Tuple2<String, BatchedCarts>> out) throws Exception {
            if ((in.getCountry() == null) && (in.getPaymentMethod() == null)) {
                for (String key : pendingBatches.keySet()) {
                    out.collect(Tuple2.of(key, pendingBatches.get(key).build()));
                }

                return;
            }

            // Get the key from the incoming record, and see if we already have a batch for it.
            String keyTemplate = String.format("%s|%s|%%d", in.getCountry(), in.getPaymentMethod());
            String key = makeKeyForOperatorIndex(keyTemplate, maxParallelism, numReports, reportNumber - 1);

            BatchedCarts.Builder builder = pendingBatches.get(totalBatchedRecords);
            if (builder == null) {
                builder = new BatchedCarts.Builder();
                pendingBatches.put(key, builder);
            }

            builder.add(in);

            // TODO - flush based on max count. Can we use LRU to figure out which one to flush?
            // We could have linked list of keys acting as LRU.
            totalBatchedRecords++;
        }

    }

    private static class ConvertToText extends KeyedProcessFunction<String, Tuple2<String, BatchedCarts>, String> {

        @Override
        public void processElement(Tuple2<String, BatchedCarts> in, Context ctx, Collector<String> out) throws Exception {
            for (ECommerceRecord record : in.f1) {
                out.collect(record.toString());
            }
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