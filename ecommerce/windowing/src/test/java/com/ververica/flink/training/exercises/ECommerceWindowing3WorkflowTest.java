package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.solutions.ECommerceWindowingSolution3Workflow;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class ECommerceWindowing3WorkflowTest {

    @Test
    public void testECommerceWindowingWorkflow() throws Exception {
        List<ShoppingCartRecord> records = ECommerceTestUtils.makeCartRecords();

        OneMinuteSink oneMinuteSink = new OneMinuteSink();
        FiveMinuteSink fiveMinuteSink = new FiveMinuteSink();
        LongestTransactionSink longestTransactionSink = new LongestTransactionSink();

        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism 2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);
        new ECommerceWindowing3Workflow()
                .setCartStream(env.fromData(records).setParallelism(1))
                .setOneMinuteSink(oneMinuteSink)
                .setFiveMinuteSink(fiveMinuteSink)
                .setLongestTransactionsSink(longestTransactionSink)
                .build();

        env.execute("ECommerceWindowing3Job");

        ECommerceTestUtils.validateOneMinuteResults(oneMinuteSink.getSink());
        ECommerceTestUtils.validateFiveMinuteResults(fiveMinuteSink.getSink());
        ECommerceTestUtils.validateLongestTransactionResults(longestTransactionSink.getSink());
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