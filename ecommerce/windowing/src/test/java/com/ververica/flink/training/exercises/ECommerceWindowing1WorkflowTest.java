package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class ECommerceWindowing1WorkflowTest {

    @Test
    public void testAggregation() throws Exception {
        List<ShoppingCartRecord> records = ECommerceTestUtils.makeCartRecords();

        ResultsSink sink = new ResultsSink();

        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism 2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);
        new ECommerceWindowing1Workflow()
                .setCartStream(env.fromData(records).setParallelism(1))
                .setResultSink(sink)
                .build();

        env.execute("ECommerceWindowing1Job");

        ECommerceTestUtils.validateOneMinuteResults(sink.getSink());
    }

    private static class ResultsSink extends MockSink<KeyedWindowResult> {

        private static ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
            return QUEUE;
        }
    }
}