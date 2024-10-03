package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class ECommerceWindowingSolution2WorkflowTest {

    @Test
    public void testAggregation() throws Exception {
        List<ShoppingCartRecord> records = ECommerceTestUtils.makeCartRecords();

        OneMinuteSink oneMinuteSink = new OneMinuteSink();
        FiveMinuteSink fiveMinuteSink = new FiveMinuteSink();

        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism 2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);
        new ECommerceWindowingSolution2Workflow()
                .setCartStream(env.fromData(records).setParallelism(1))
                .setOneMinuteSink(oneMinuteSink)
                .setFiveMinuteSink(fiveMinuteSink)
                .build();

        env.execute("ECommerceWindowingSolution2Job");

        ECommerceTestUtils.validateOneMinuteResults(oneMinuteSink.getSink());
        ECommerceTestUtils.validateFiveMinuteResults(fiveMinuteSink.getSink());
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


}