package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.provided.KeyedWindowDouble;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.provided.BootcampEnrichmentWorkflowTestUtils.testAddingProductWeightWorkflow;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class BootcampEnrichmentSolution2WorkflowTest {

    @Test
    public void testAddingProductWeight() throws Exception {
        testAddingProductWeightWorkflow(new BootcampEnrichmentSolution2Workflow());
    }
}