package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.EnvironmentUtils;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.TransactionalMemorySink;
import com.ververica.flink.training.provided.ShoppingCartFiles;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

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
        loggerConfig.setLevel(Level.INFO);
        ctx.updateLoggers();  // This causes all Loggers to refetch information from their LoggerConfig.

        ParameterTool parameters = ParameterTool.fromArgs(new String[]{
                "--parallelism", "1",
                // reduce time between restarts
                "--restartdelay", "1"});
        final StreamExecutionEnvironment env1 = EnvironmentUtils.createConfiguredLocalEnvironment(parameters);
        // Set up for exactly once mode.
        env1.enableCheckpointing(Duration.ofSeconds(10).toMillis(), CheckpointingMode.EXACTLY_ONCE);

        ConvertCartRecords function = new ConvertCartRecords();
        function.reset();

        // TODO - Have unbounded option for shopping cart source.
        final boolean unbounded = true;
        DataStream<ShoppingCartRecord> cartStream = env1.fromSource(ShoppingCartFiles.makeCartFilesSource(unbounded),
                        WatermarkStrategy.noWatermarks(),
                        "Shopping Cart Text Stream")
                .map(function)
                .name("Shopping Cart Stream")
                .setParallelism(1);


//         MemorySink resultsSink = new MemorySink();
        boolean exactlyOnce = true;
        TransactionalMemorySink resultsSink = new TransactionalMemorySink(exactlyOnce);
        resultsSink.reset();

        new ECommerceFailuresSolution1Workflow()
                .setCartStream(cartStream)
                .setResultSink(resultsSink)
                .build();

        // TODO - execute async, then wait for count to reach target or there's
        // an error
        JobClient client = env1.executeAsync("ECommerceFailuresSolution1WorkflowTest");

        int numRecords;
        while ((numRecords = function.getNumRecords()) < NUM_RECORDS * 2)  {
            System.out.println("Num records: " + numRecords);
            Thread.sleep(100L);
        }
        client.cancel();

        System.out.println(resultsSink.getSink());
        System.out.println(function.getNumRecords());

        assertThat(resultsSink.getSink()).containsExactlyInAnyOrder(
                new KeyedWindowResult("CA", START_TIME, 80L).toString(),
                new KeyedWindowResult("CN", START_TIME, 174L).toString(),
                new KeyedWindowResult("CN", START_TIME + Duration.ofMinutes(1).toMillis(), 14L).toString(),
                new KeyedWindowResult("JP", START_TIME, 8L).toString(),
                new KeyedWindowResult("JP", START_TIME + Duration.ofMinutes(1).toMillis(), 8L).toString(),
                new KeyedWindowResult("MX", START_TIME, 234L).toString(),
                new KeyedWindowResult("MX", START_TIME + Duration.ofMinutes(1).toMillis(), 25L).toString(),
                new KeyedWindowResult("US", START_TIME, 773L).toString(),
                new KeyedWindowResult("US", START_TIME + Duration.ofMinutes(1).toMillis(), 104L).toString()
        );


    }

    private static class ConvertCartRecords implements MapFunction<String, ShoppingCartRecord> {

        private static final AtomicInteger NUM_RECORDS = new AtomicInteger();

        public static void reset() {
            NUM_RECORDS.set(0);
        }

        @Override
        public ShoppingCartRecord map(String s) throws Exception {
            ShoppingCartRecord result = ShoppingCartRecord.fromString(s);
            NUM_RECORDS.incrementAndGet();
            return result;
        }

        public int getNumRecords() {
            return NUM_RECORDS.get();
        }
    }

}