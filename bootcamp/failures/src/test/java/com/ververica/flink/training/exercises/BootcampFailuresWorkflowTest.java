package com.ververica.flink.training.exercises;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampFailuresWorkflowTestUtils.testWorkflow;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BootcampFailuresWorkflowTest {

    @Test
    public void testGettingCorrectResultsAfterFailure() throws Exception {
        testWorkflow(new BootcampFailuresConfig(), true);
    }

    @Test
    public void testLatency() throws Exception {
        testWorkflow(new BootcampFailuresConfig(), false);
    }
}