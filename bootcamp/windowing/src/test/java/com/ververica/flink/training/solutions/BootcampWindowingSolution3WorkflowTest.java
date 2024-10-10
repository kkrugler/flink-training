package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.exercises.BootcampWindowing3WorkflowTest;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class BootcampWindowingSolution3WorkflowTest extends BootcampWindowing3WorkflowTest {

    @Test
    public void testBootcampWindowingSolution3Workflow() throws Exception {
        testBootcampWindowing3Workflow(new BootcampWindowingSolution3Workflow());
    }
}