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
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ververica.flink.training.provided.ECommerceRecord;

import java.util.ArrayList;
import java.util.List;

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
    protected List<ReportBy> reports = new ArrayList<>();

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

    public BootcampSortingSolutionWorkflow addReport(ReportBy reportBy) {
        this.reports.add(reportBy);
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultsSink, "resultsSink must be set");
        Preconditions.checkArgument(maxParallelism > 0, "Max parallelism must be set");

        final int numReports = reports.size();
        Preconditions.checkArgument(numReports > 0);

        // Do a map-side pre-sort, where we group records into "batches" that all
        // share the same top-level sorting key.
        DataStream<Tuple2<Integer, BatchedCarts>> batched = cartStream
                .flatMap(new CreateBatchedCarts(reports));

        batched
                .partitionCustom(new PartitionByReport(), t -> t.f0)
                .process(new MergeSortRecords(reports))
                .setParallelism(numReports)
                .flatMap(new CreateTSVRecord())
                .setParallelism(numReports)
                .sinkTo(resultsSink)
                .setParallelism(numReports);
    }

    private static class PartitionByReport implements Partitioner<Integer> {

        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
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

}