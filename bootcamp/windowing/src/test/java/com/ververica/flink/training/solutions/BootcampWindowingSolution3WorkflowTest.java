package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.exercises.BootcampWindowing3WorkflowTest;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.exercises.BootcampWindowing3WorkflowTest.testBootcampWindowing3Workflow;

class BootcampWindowingSolution3WorkflowTest {

    @Test
    public void testBootcampWindowingSolution3Workflow() throws Exception {
        testBootcampWindowing3Workflow(new BootcampWindowingSolution3Workflow());
    }
}