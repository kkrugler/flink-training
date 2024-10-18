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
import com.ververica.flink.training.provided.BootcampDesignDetectionWorkflow;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.jetbrains.annotations.Nullable;
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

/*
 * Solution to the workflow design exercise. We
 */
public class BootcampDesignSolutionTest {

    @Test
    public void testBridgingWorkflows() throws Exception {
        final StreamExecutionEnvironment env1 = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        final StreamExecutionEnvironment env2 = FlinkClusterUtils.createConfiguredTestEnvironment(2);

        // FIXME - use files that have a significant percentage of uncompleted transactions.

        FileSource<String> carts = makeCartFilesSource(false);
        DataStream<ShoppingCartRecord> cartStream = env1.fromSource(carts,
                        WatermarkStrategy.noWatermarks(), "shopping carts")
                .map(s -> ShoppingCartRecord.fromString(s));

        Path resultsDir = new Path(Files.createTempDirectory("temp").toUri());
        // FIXME - create Paimon sink

        FileSink<String> abandonedSink = FileSink.forRowFormat(resultsDir,
                new SimpleStringEncoder<String>()).build();

        new BootcampDesignAnalyticsSolutionWorkflow()
                .setCartStream(cartStream)
                // Ignore the results for this test
                .setResultSink(new DiscardingSink<>())
                .setAbandonedSink(abandonedSink)
                .build();

        ResultsSink analyticsResults = new ResultsSink();

        // FIXME - create Paimon source
        FileSource<String> abandonedSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat("UTF-8"), resultsDir).build();

        new BootcampDesignDetectionWorkflow()
                .setAbandonedStream(env2.fromSource(abandonedSource, WatermarkStrategy.noWatermarks(),
                        "abandoned products"))
                .setResultSink(analyticsResults)
                .build();

        // Run async, so we can have both jobs running at the same time.
        env1.executeAsync("BootcampDesignAnalyticsWorkflow");
        env2.executeAsync("BootcampDesignDetectionWorkflow");

        // Wait for data to finish being consumed
        Thread.sleep(2000L);

        System.out.println("Results: " + analyticsResults.getSink());
        for (KeyedWindowResult result : analyticsResults.getSink()) {
            System.out.println(result);
        }
        // FIXME - validate results.

    }

    private static class ResultsSink extends MockSink<KeyedWindowResult> {

        private static ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
            return QUEUE;
        }
    }

    public static FileSource<String> makeCartFilesSource(boolean unbounded) throws URISyntaxException {
        // FIXME - use path in our resource dir
        URL srcPathAsURL = BootcampDesignSolutionTest.class.getResource("/cart-files/file-000.txt");
        Path srcPath = new Path(srcPathAsURL.toURI());

        // Create a stream of ShoppingCartRecords from the directory we just filled with files.
        FileSource.FileSourceBuilder<String> builder = FileSource.forRecordStreamFormat(
                new TextLineInputFormat("UTF-8"),
                srcPath.getParent());

        if (unbounded) {
            builder.monitorContinuously(Duration.ofSeconds(10));
        } else {
            builder.processStaticFileSet();
        }

        return builder.build();
    }


}