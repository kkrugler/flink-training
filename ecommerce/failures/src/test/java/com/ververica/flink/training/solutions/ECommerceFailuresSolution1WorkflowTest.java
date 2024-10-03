package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.EnvironmentUtils;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.MockSink;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.ShoppingCartFiles;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class ECommerceFailuresSolution1WorkflowTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ECommerceFailuresSolution1WorkflowTest.class);

    // The beginning time for our workflow, for events
    private static final long START_TIME = 0;
    private static final long NUM_RECORDS = 1_000;

    @Test
    public void testGettingCorrectResultsAfterFailure() throws Exception {
        // TODO - remove this, once log4j is configured properly.
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.WARN);
        ctx.updateLoggers();  // This causes all Loggers to refetch information from their LoggerConfig.

        ParameterTool parameters = ParameterTool.fromArgs(new String[]{
                "--parallelism 1"});
        final StreamExecutionEnvironment env1 = EnvironmentUtils.createConfiguredLocalEnvironment(parameters);
        // Set up for exactly once mode.
        // TODO - env.enableCheckpointing(Duration.ofSeconds(5).toMillis(), CheckpointingMode.EXACTLY_ONCE);

        DataStream<ShoppingCartRecord> cartStream = env1.fromSource(ShoppingCartFiles.makeCartFilesSource(),
                        WatermarkStrategy.noWatermarks(),
                        "Shopping Cart Text Stream")
                .map(s -> ShoppingCartRecord.fromString(s))
                .name("Shopping Cart Stream")
                .setParallelism(1);

        Path destDir = new Path(Files.createTempDirectory("results").toUri());
        Sink<String> resultsSink = ShoppingCartFiles.makeResultFilesSink(destDir);

        new ECommerceFailuresSolution1Workflow()
                .setCartStream(cartStream)
                .setResultSink(resultsSink)
                .build();

        LOGGER.warn("Generating results to {}", destDir.getPath());
        env1.execute("ECommerceFailuresSolution1WorkflowTest-generate results");

        // Run job to get results.
        LOGGER.warn("Reading results");
        final StreamExecutionEnvironment env2 = EnvironmentUtils.createConfiguredLocalEnvironment(parameters);
        Iterator<KeyedWindowResult> results = env2.fromSource(ShoppingCartFiles.makeResultFilesSource(destDir),
                        WatermarkStrategy.noWatermarks(),
                        "KeyedWindowResult Text Stream")
                .map(s -> KeyedWindowResult.fromString(s))
                .executeAndCollect("ECommerceFailuresSolution1WorkflowTest-validate results");

        List<KeyedWindowResult> resultsList = new ArrayList<>();
        results.forEachRemaining(resultsList::add);

        LOGGER.info("Validating results");
        assertThat(resultsList).containsExactlyInAnyOrder(
                new KeyedWindowResult("CA", START_TIME, 80L),
                new KeyedWindowResult("CN", START_TIME, 174L),
                new KeyedWindowResult("CN", START_TIME + Duration.ofMinutes(1).toMillis(), 14L),
                new KeyedWindowResult("JP", START_TIME, 8L),
                new KeyedWindowResult("JP", START_TIME + Duration.ofMinutes(1).toMillis(), 8L),
                new KeyedWindowResult("MX", START_TIME, 234L),
                new KeyedWindowResult("MX", START_TIME + Duration.ofMinutes(1).toMillis(), 25L),
                new KeyedWindowResult("US", START_TIME, 773L),
                new KeyedWindowResult("US", START_TIME + Duration.ofMinutes(1).toMillis(), 104L)
        );
    }

}