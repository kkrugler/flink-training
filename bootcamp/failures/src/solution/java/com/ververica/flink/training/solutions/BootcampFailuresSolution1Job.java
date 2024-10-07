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
import com.ververica.flink.training.provided.ShoppingCartFilesGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ECommerceFailuresSolution1Job {

    private static final long START_TIME = 0;

    public static void main(String[] args) throws Exception {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.INFO);
        ctx.updateLoggers();  // This causes all Loggers to refetch information from their LoggerConfig.

        ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredLocalEnvironment(parameters);
        // TODO - move most of this to a test (util class, for both exercise and solution)
        // Set up for exactly once mode.
        env.enableCheckpointing(Duration.ofSeconds(5).toMillis(), CheckpointingMode.EXACTLY_ONCE);

        // Create a directory with shopping cart files
        Path srcDir = Files.createTempDirectory("shopping-cart-files");
        final long numRecords = 10_000;
        final long numFiles = 10;
        ShoppingCartGenerator generator = new ShoppingCartGenerator(START_TIME);
        ShoppingCartFilesGenerator.generateFiles(generator, srcDir.toFile(), numRecords, numFiles);

        // Create a stream of ShoppingCartRecords from the directory we just filled with files.
        FileSource<String> cartsAsText = FileSource.forRecordStreamFormat(new TextLineInputFormat("UTF-8"),
                        new org.apache.flink.core.fs.Path(srcDir.toUri()))
                .build();

        // TODO - have watermark generator that occasionally fails.
        DataStream<ShoppingCartRecord> cartStream = env.fromSource(cartsAsText,
                        WatermarkStrategy.noWatermarks(),
                        "Shopping Cart Text Stream")
                .setParallelism(1)
                .map(r -> ShoppingCartRecord.fromString(r))
                .name("Shopping Cart Stream");

        new ECommerceFailuresSolution1Workflow()
                .setCartStream(cartStream)
                .setResultSink(new DiscardingSink<>())
                .build();

        env.execute("ECommerceFailuresSolution1Job");

    }

    private static class CountingSink extends MockSink<KeyedWindowResult> {

        private static final ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
            return QUEUE;
        }
    }

}