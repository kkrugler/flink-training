package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BootcampWindowing3WorkflowTest {

    @Test
    public void testBootcampWindowing3Workflow() throws Exception {
        testBootcampWindowing3Workflow(new BootcampWindowing3Workflow());
    }

    public static void testBootcampWindowing3Workflow(BootcampWindowing3Workflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        OneMinuteSink oneMinuteSink = new OneMinuteSink();
        FiveMinuteSink fiveMinuteSink = new FiveMinuteSink();
        LongestTransactionSink longestTransactionSink = new LongestTransactionSink();

        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism", "2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);
        workflow
                .setCartStream(env.fromData(records).setParallelism(1))
                .setOneMinuteSink(oneMinuteSink)
                .setFiveMinuteSink(fiveMinuteSink)
                .setLongestTransactionsSink(longestTransactionSink)
                .build();

        env.execute("BootcampWindowing3Job");

        BootcampTestUtils.validateOneMinuteResults(oneMinuteSink.getSink());
        BootcampTestUtils.validateFiveMinuteResults(fiveMinuteSink.getSink());
        BootcampTestUtils.validateLongestTransactionResults(longestTransactionSink.getSink());
    }

    private static class OneMinuteSink extends MockSink<KeyedWindowResult> {

        private static ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
            return QUEUE;
        }
    }

    private static class FiveMinuteSink extends MockSink<WindowAllResult> {

        private static ConcurrentLinkedQueue<WindowAllResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<WindowAllResult> getSink() {
            return QUEUE;
        }
    }

    private static class LongestTransactionSink extends MockSink<KeyedWindowResult> {

        private static ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
            return QUEUE;
        }
    }

}