package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampSerializationWorkflowTestUtils.testWorkflow;

class BootcampSerializationSolution1WorkflowTest {

    @Test
    public void testBootcampSerializationSolution1Workflow() throws Exception {
        testWorkflow(new BootcampSerializationSolution1Workflow());
    }
}
