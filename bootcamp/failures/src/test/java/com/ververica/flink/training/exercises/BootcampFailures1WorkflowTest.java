package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.FlinkClusterUtils;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.ShoppingCartFiles;
import com.ververica.flink.training.provided.TransactionalMemorySink;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BootcampFailures1WorkflowTest {

    // The beginning time for our workflow, for events
    private static final long START_TIME = 0;
    private static final long NUM_RECORDS = 1_000;

    @Test
    public void testGettingCorrectResultsAfterFailure() throws Exception {
        testGettingCorrectResultsAfterFailure(new BootcampFailuresConfig());
    }

    public static void testGettingCorrectResultsAfterFailure(BootcampFailuresConfig config) throws Exception {
        final StreamExecutionEnvironment env = config.getEnvironment();

        ConvertCartRecords convertFunction = new ConvertCartRecords();

        final boolean unbounded = true;
        DataStream<ShoppingCartRecord> cartStream = env.fromSource(ShoppingCartFiles.makeCartFilesSource(unbounded),
                        WatermarkStrategy.noWatermarks(),
                        "Shopping Cart Text Stream")
                .map(convertFunction)
                .name("Shopping Cart Stream")
                .setParallelism(1);

        TransactionalMemorySink resultsSink = config.getResultsSink();
        resultsSink.reset();

        new BootcampFailuresWorkflow()
                .setCartStream(cartStream)
                .setResultSink(resultsSink)
                .build();

        // Wait for the job to restart, due to our one-time failure.
        JobClient client = env.executeAsync("BootcampFailuresWorkflowTest");
        while (!client.getJobStatus().isDone() && (client.getJobStatus().get() != JobStatus.RESTARTING)) {
            Thread.sleep(100L);
        }

        // Wait for the job to be running normally.
        while (!client.getJobStatus().isDone() && (client.getJobStatus().get() != JobStatus.RUNNING)) {
            Thread.sleep(100L);
        }

        // Wait until at least N checkpoints have happened (actually n-1), so that we know the sink
        // has had a commit call.
        final int numCheckpoints = 3;
        File checkpointDir = new File(new URI(env.getConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY)));
        long endTime = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
        while (!findCheckpointDir(checkpointDir, numCheckpoints) && (System.currentTimeMillis() < endTime))  {
            Thread.sleep(200L);
        }

        // Stop the workflow if it's still running.
        if (!client.getJobStatus().isDone()) {
            client.cancel();
        }

        assertThat(resultsSink.getCommitted()).containsExactlyInAnyOrder(
                new KeyedWindowResult("CA", START_TIME, 80L).toString(),
                new KeyedWindowResult("CN", START_TIME, 174L).toString(),
                new KeyedWindowResult("CN", START_TIME + Duration.ofMinutes(1).toMillis(), 14L).toString(),
                new KeyedWindowResult("JP", START_TIME, 8L).toString(),
                new KeyedWindowResult("JP", START_TIME + Duration.ofMinutes(1).toMillis(), 8L).toString(),
                new KeyedWindowResult("MX", START_TIME, 234L).toString(),
                new KeyedWindowResult("MX", START_TIME + Duration.ofMinutes(1).toMillis(), 25L).toString(),
                new KeyedWindowResult("US", START_TIME, 773L).toString(),
                new KeyedWindowResult("US", START_TIME + Duration.ofMinutes(1).toMillis(), 104L).toString(),

                // We have a special 0 count entry that triggers the one-time exception.
                new KeyedWindowResult("MX", START_TIME + Duration.ofMinutes(1025).toMillis(), 0L).toString()
        );

    }

    private static boolean findCheckpointDir(File checkpointDir, int checkpointNumber) {
        String targetDirName = String.format("chk-%d", checkpointNumber);
        LinkedList<File> stack = new LinkedList<>();
        stack.push(checkpointDir);

        while (!stack.isEmpty()) {
            File curDirectory = stack.pop();
            for (File f : curDirectory.listFiles()) {
                if (f.isDirectory()) {
                    if (f.getName().equals(targetDirName)) {
                        return true;
                    }

                    stack.push(f);
                }
            }
        }

        return false;
    }

    private static class ConvertCartRecords implements MapFunction<String, ShoppingCartRecord> {

        private static final AtomicInteger NUM_RECORDS = new AtomicInteger();

        public static void reset() {
            NUM_RECORDS.set(0);
        }

        public ConvertCartRecords() {
            reset();
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