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

package com.ververica.flink.training.common;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Duration;

import static org.apache.flink.configuration.RestOptions.BIND_PORT;
import static org.apache.flink.configuration.RestOptions.ENABLE_FLAMEGRAPH;
import static org.apache.flink.configuration.TaskManagerOptions.*;

/** Common functionality to set up execution environments for the eCommerce training. */
public class EnvironmentUtils {
    public static final Logger LOG = LoggerFactory.getLogger(EnvironmentUtils.class);

    private static final String NO_WEBUI_PORT = "-1";

    public static StreamExecutionEnvironment createConfiguredLocalEnvironment(
            final ParameterTool parameters) throws IOException, URISyntaxException {
        return createEnvironment(parameters, BIND_PORT.defaultValue());
    }

            /**
             * Creates a streaming environment with a few pre-configured settings based on command-line
             * parameters.
             *
             * @throws IOException if the local checkpoint directory for the file system state backend*
             *     cannot be created
             * @throws URISyntaxException if <code>fsStatePath</code> is not a valid URI
             */
    public static StreamExecutionEnvironment createConfiguredEnvironment(
            final ParameterTool parameters) throws IOException, URISyntaxException {
        // TODO - use isLocal(parameters)
        final String localMode =
                parameters.get(
                        "local",
                        System.getenv("FLINK_TRAINING_LOCAL") != null
                                ? BIND_PORT.defaultValue()
                                : NO_WEBUI_PORT);

        return createEnvironment(parameters, localMode);
    }

    private static StreamExecutionEnvironment createEnvironment(
            final ParameterTool parameters, String uiPort) throws IOException, URISyntaxException {
        final StreamExecutionEnvironment env;
        if (uiPort.equals(NO_WEBUI_PORT)) {
            // cluster mode or disabled web UI
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            // configure Web UI
            Configuration flinkConfig = new Configuration();
            flinkConfig.set(BIND_PORT, uiPort);
            flinkConfig.set(CPU_CORES, 4.0);
            flinkConfig.set(TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
            flinkConfig.set(TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(256));
            flinkConfig.set(MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(1024));

            // Enable Flamegraphs
            flinkConfig.set(ENABLE_FLAMEGRAPH, true);

            // configure directory for JobManager log files
            Files.createTempDirectory("flink-logfiles");
            System.setProperty("log.file", Files.createTempDirectory("flink-logfiles").toString());

            // configure filesystem state backend
            String statePath = parameters.get("fsStatePath");
            Path checkpointPath;
            if (statePath != null) {
                FileUtils.deleteDirectory(new File(new URI(statePath)));
                checkpointPath = Path.fromLocalFile(new File(new URI(statePath)));
            } else {
                checkpointPath =
                        Path.fromLocalFile(Files.createTempDirectory("flink-checkpoints").toFile());
            }

            if (parameters.has("useRocksDB")) {
                flinkConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            } else {
                flinkConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
            }

            LOG.info("Writing checkpoints to {}", checkpointPath);
            flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath.toString());

            // set a restart strategy for better IDE debugging
            flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
            flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(15));

            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        }

        final int parallelism = parameters.getInt("parallelism", -1);
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }

        env.getConfig().setGlobalJobParameters(parameters);
        return env;
    }

    /** Checks whether the environment should be set up in local mode (with Web UI,...). */
    public static boolean isLocal(ParameterTool parameters) {
        final String localMode = parameters.get("local");
        if (localMode == null) {
            return System.getenv("FLINK_TRAINING_LOCAL") != null;
        } else {
            return !localMode.equals(NO_WEBUI_PORT);
        }
    }
}
