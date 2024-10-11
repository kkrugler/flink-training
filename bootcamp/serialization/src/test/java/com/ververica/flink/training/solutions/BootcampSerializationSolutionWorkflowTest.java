package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.exercises.BootcampSerializationWorkflowTest.testBootcampSerializationWorkflow;

class BootcampSerializationSolutionWorkflowTest {

    @Test
    public void testBootcampSerializationSolutionWorkflow() throws Exception {
        testBootcampSerializationWorkflow(new BootcampSerializationSolutionWorkflow());
    }
}