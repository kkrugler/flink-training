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

import com.ververica.flink.training.common.FlinkClusterUtils;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.common.ShoppingCartSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.time.Duration;

public class BootcampSortingSolutionJob {

    public static void main(String[] args) throws Exception {
        final boolean discarding = false; // We always want to discard, to avoid performance impact from printing.
        final int parallelism = 2;
        final long numRecords = 1_000; // Set to 0 for unbounded source
        final int maxParallelism = 400;

        ParameterTool parameters = ParameterTool.fromArgs(args);
//        Configuration config = new Configuration();
//        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
//        config.set(ExecutionOptions.)
        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredLocalEnvironment(parameters, parallelism);
        env.setMaxParallelism(maxParallelism);

        final boolean bounded = numRecords != 0L;
        ShoppingCartSource source = bounded ? new ShoppingCartSource(numRecords, 0L) : new ShoppingCartSource();

        WatermarkStrategy<ShoppingCartRecord> wmStrategy = WatermarkStrategy
                .<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime());

        // TODO - support writing to a FileSink
        new BootcampSortingSolutionWorkflow()
                .setCartStream(env.fromSource(source, wmStrategy, "Shopping Cart Stream"))
                .setResultsSink(discarding ? new DiscardingSink<>() : new PrintSink<>())
                .setMaxParallelism(maxParallelism)
                .build();

        JobExecutionResult jobResult = env.execute("BootcampSortingSolutionJob");
    }

}