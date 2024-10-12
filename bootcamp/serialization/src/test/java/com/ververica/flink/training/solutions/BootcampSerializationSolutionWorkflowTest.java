package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.exercises.BootcampSerializationWorkflowTest.testBootcampSerializationWorkflow;

class BootcampSerializationSolutionWorkflowTest {

    @Test
    public void testBootcampSerializationSolutionWorkflow() throws Exception {
        testBootcampSerializationWorkflow(new BootcampSerializationSolutionWorkflow());
    }
}
