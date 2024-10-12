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
import com.ververica.flink.training.common.ShoppingCartSource;
import com.ververica.flink.training.provided.BootcampDesignDetectionWorkflow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

/*
 * Solution to the workflow design exercise. We
 */
public class BootcampDesignSolutionTest {

    @Test
    public void testBridgingWorkflows() throws Exception {
        final StreamExecutionEnvironment env1 = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        final StreamExecutionEnvironment env2 = FlinkClusterUtils.createConfiguredTestEnvironment(2);

        // TODO use pre-defined set of records to test
        new BootcampDesignAnalyticsSolutionWorkflow()
                .setCartStream(env1.fromSource(new ShoppingCartSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Shopping Cart Stream"))
                // Ignore the results for this test
                .setResultSink(new DiscardingSink<>())
                .setAbandonedSink(/* TODO - create Paimon sink null */ null)
                .build();

        new BootcampDesignDetectionWorkflow()
                .setAbandonedStream(/* TODO - create Paimon source */ null)
                // TODO - save results, validate
                .setResultSink(new DiscardingSink<>())
                .build();

        // Run async, so we can have both jobs running at the same time.
        JobClient client1 = env1.executeAsync("BootcampDesignAnalyticsWorkflow");
        JobClient client2 = env2.executeAsync("BootcampDesignDetectionWorkflow");

        while (!client1.getJobStatus().isDone() && !client2.getJobStatus().isDone()) {
            Thread.sleep(100);
        }

        Assert.assertFalse(client1.getJobStatus().isCompletedExceptionally());
        Assert.assertFalse(client2.getJobStatus().isCompletedExceptionally());

        // TODO - validate results.

    }
}