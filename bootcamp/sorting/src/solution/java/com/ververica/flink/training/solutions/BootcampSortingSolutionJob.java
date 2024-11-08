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
import com.ververica.flink.training.provided.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
        final long numRecords = 5_000_000;
        final int maxParallelism = 400;

        ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredLocalEnvironment(parameters);
        env.setMaxParallelism(maxParallelism);

        ECommerceSource realSource = new ECommerceSource(numRecords);
        ECommerceEndSource endSource = new ECommerceEndSource(env.getParallelism());

        HybridSource<ECommerceRecord> realPlusEnd = HybridSource.builder(realSource)
                .addSource(endSource)
                .build();

        DataStream<ECommerceRecord> records = env.fromSource(realPlusEnd,
                            WatermarkStrategy.noWatermarks(),
                "ECommerce Stream",
                            TypeInformation.of(ECommerceRecord.class));

        new BootcampSortingSolutionWorkflow()
                .setCartStream(records)
                // TODO - support writing to a FileSink
                .setResultsSink(discarding ? new DiscardingSink<>() : new PrintSink<>())
                .setBatchingParallelism(env.getParallelism())
                .setMaxParallelism(env.getMaxParallelism())
                .addReport(new ReportByCountrySortByShippingCost())
                .addReport(new ReportByCustomerIdSortByTransactionTime())
                .build();

        // Verify no acc is expected.
        JobExecutionResult jobResult = env.execute("BootcampSortingSolutionJob");
        Map<String, Object> acc = jobResult.getAllAccumulatorResults();
        for (String accKey : acc.keySet()) {
            System.out.format("%s: %s\n", accKey, acc.get(accKey));
        }
    }

}