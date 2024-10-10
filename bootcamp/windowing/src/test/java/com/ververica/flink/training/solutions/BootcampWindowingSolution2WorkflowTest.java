package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.exercises.BootcampWindowing2WorkflowTest;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class BootcampWindowingSolution2WorkflowTest extends BootcampWindowing2WorkflowTest {

    @Test
    public void testBootcampWindowingSolution2Workflow() throws Exception {
        testBootcampWindowing2Workflow(new BootcampWindowingSolution2Workflow());
    }

}