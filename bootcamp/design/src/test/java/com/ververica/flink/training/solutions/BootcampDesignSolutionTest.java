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
import com.ververica.flink.training.provided.AbandonedCartItem;
import com.ververica.flink.training.provided.BootcampDesignDetectionWorkflow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.assertFalse;

/*
 * Solution to the workflow design exercise. We
 */
public class BootcampDesignSolutionTest {

    private static final long START_TIME = 0L;

    @Test
    public void testBridgingWorkflows() throws Exception {
        final StreamExecutionEnvironment env1 = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        env1.enableCheckpointing(100L, CheckpointingMode.EXACTLY_ONCE);
        final StreamExecutionEnvironment env2 = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        env2.enableCheckpointing(100L, CheckpointingMode.EXACTLY_ONCE);

        // FIXME - use list with specific records (one that is completed and thus ignored, three that are
        // abandoned in time window 0, and 1 that is abandoned in time window 1
        FileSource<String> carts = makeCartFilesSource(false);
        DataStream<ShoppingCartRecord> cartStream = env1.fromSource(carts,
                        WatermarkStrategy.noWatermarks(), "shopping carts")
                .map(s -> ShoppingCartRecord.fromString(s));

        Path resultsDir = new Path(Files.createTempDirectory("temp").toUri());
        System.out.println("Saving abandoned products at " + resultsDir);
        FileSink<String> abandonedSink = FileSink.forRowFormat(resultsDir, new SimpleStringEncoder<String>())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        new BootcampDesignAnalyticsSolutionWorkflow()
                .setCartStream(cartStream)
                // Ignore the results for this test
                .setResultSink(new DiscardingSink<>())
                .setAbandonedSink(abandonedSink)
                .build();

        ResultsSink analyticsResults = new ResultsSink();

        FileSource<String> abandonedSource = FileSource.forRecordStreamFormat(new TextLineInputFormat("UTF-8"), resultsDir)
                .monitorContinuously(Duration.ofMillis(100))
                .build();

        // Create a watermarked stream of AbandonedCartItem records.
        DataStream<AbandonedCartItem> abandonedStream = env2.fromSource(abandonedSource,
                WatermarkStrategy.noWatermarks(), "abandoned products")
                .map(s -> AbandonedCartItem.fromString(s))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AbandonedCartItem>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .setParallelism(1);

        new BootcampDesignDetectionWorkflow()
                .setAbandonedStream(abandonedStream)
                .setResultSink(analyticsResults)
                .build();

        // Run async, so we can have both jobs running at the same time.
        JobClient client1 = env1.executeAsync("BootcampDesignAnalyticsWorkflow");
        JobClient client2 = env2.executeAsync("BootcampDesignDetectionWorkflow");

        // Wait for data to finish being consumed
        // FIXME - reduce time significantly.
        Thread.sleep(1000L);

        assertFalse(client1.getJobExecutionResult().isCompletedExceptionally());
        assertFalse(client2.getJobExecutionResult().isCompletedExceptionally());

        // FIXME - match results to sample data.
//        assertThat(analyticsResults.getSink()).containsExactlyInAnyOrder(
//                new KeyedWindowResult("C37119", START_TIME, 287),
//                new KeyedWindowResult("C10381", START_TIME, 480),
//                new KeyedWindowResult("C64921", START_TIME, 128),
//                new KeyedWindowResult("C29975", START_TIME, 225)
//        );
    }

    private static class ResultsSink extends MockSink<KeyedWindowResult> {

        private static ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
            return QUEUE;
        }
    }

    public static FileSource<String> makeCartFilesSource(boolean unbounded) throws URISyntaxException {
        URL srcPathAsURL = BootcampDesignSolutionTest.class.getResource("/cart-files/file-001.gz");
        Path srcPath = new Path(srcPathAsURL.toURI()).getParent();

        // Create a stream of ShoppingCartRecords from our directory of string-ified ShoppingCartRecords.
        FileSource.FileSourceBuilder<String> builder = FileSource.forRecordStreamFormat(
                new TextLineInputFormat("UTF-8"),
                srcPath);

        if (unbounded) {
            builder.monitorContinuously(Duration.ofSeconds(10));
        } else {
            builder.processStaticFileSet();
        }

        return builder.build();
    }


}