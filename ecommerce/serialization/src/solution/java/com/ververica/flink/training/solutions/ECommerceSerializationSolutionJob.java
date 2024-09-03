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

import com.ververica.flink.training.common.EnvironmentUtils;
import com.ververica.flink.training.common.ShoppingCartSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

public class ECommerceSerializationSolutionJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredLocalEnvironment(parameters);

        final boolean discarding = parameters.has("discard");

        new ECommerceSerializationSolutionWorkflow()
                .setCartStream(env.fromSource(new ShoppingCartSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Shopping Cart Stream"))
                .setOneMinuteSink(discarding ? new DiscardingSink<>() : new PrintSink<>("1m count"))
                .setFiveMinuteSink(discarding ? new DiscardingSink<>() : new PrintSink<>("5m count"))
                .setLongestTransactionsSink(discarding ? new DiscardingSink<>() : new PrintSink<>("5m longest"))
                .build();

        env.execute("ECommerceSerializationSolutionJob");
    }

}