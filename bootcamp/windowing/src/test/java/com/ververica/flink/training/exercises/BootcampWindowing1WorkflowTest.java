package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BootcampWindowing1WorkflowTest {

    @Test
    public void testBootcampWindowing1Workflow() throws Exception {
        testBootcampWindowing1Workflow(new BootcampWindowing1Workflow());
    }

    public static void testBootcampWindowing1Workflow(BootcampWindowing1Workflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        ResultsSink sink = new ResultsSink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow
                .setCartStream(env.fromData(records).setParallelism(1))
                .setResultSink(sink)
                .build();

        env.execute("BootcampWindowing1Job");

        BootcampTestUtils.validateOneMinuteResults(sink.getSink());
    }

    private static class ResultsSink extends MockSink<KeyedWindowResult> {

        private static ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
            return QUEUE;
        }
    }
}