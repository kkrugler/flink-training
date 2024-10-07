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

package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.EnvironmentUtils;
import com.ververica.flink.training.common.ShoppingCartSource;
import com.ververica.flink.training.provided.BootcampDesignDetectionWorkflow;
import com.ververica.flink.training.solutions.BootcampDesignAnalyticsSolutionWorkflow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

/*
 * Job for the workflow design exercise. We need to use a Paimon table as the
 * bridge between the two workflows.
 *
 */
public class BootcampDesignJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env1 = EnvironmentUtils.createConfiguredLocalEnvironment(parameters);
        final StreamExecutionEnvironment env2 = EnvironmentUtils.createConfiguredLocalEnvironment(parameters);

        final boolean discarding = parameters.has("discard");

        new BootcampDesignAnalyticsWorkflow()
                .setCartStream(env1.fromSource(new ShoppingCartSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Shopping Cart Stream"))
                .setResultSink(discarding ? new DiscardingSink<>() : new PrintSink<>())
                // TODO - create Paimon sink, use it in the workflow for abandoned items
                .build();

        new BootcampDesignDetectionWorkflow()
                .setAbandonedStream(/* TODO - create Paimon source from same table as above */ null)
                .setResultSink(discarding ? new DiscardingSink<>() : new PrintSink<>())
                .build();

        // Run async, so we can have both jobs running at the same time.
        env1.executeAsync("BootcampDesignAnalyticsSolutionJob");

        env2.executeAsync("BootcampDesignDetectionSolutionJob");
    }
}