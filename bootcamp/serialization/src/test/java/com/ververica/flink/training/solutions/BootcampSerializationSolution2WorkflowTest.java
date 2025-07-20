package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampSerializationWorkflowTestUtils.testWorkflow;

class BootcampSerializationSolution2WorkflowTest {

    @Test
    public void testBootcampSerializationSolution2Workflow() throws Exception {
        testWorkflow(new BootcampSerializationSolution2Workflow());
    }
}
