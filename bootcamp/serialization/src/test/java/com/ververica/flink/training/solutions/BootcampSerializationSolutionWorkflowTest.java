/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampSerializationWorkflowTestUtils.testWorkflow;

class BootcampSerializationSolutionWorkflowTest {

    @Test
    public void testBootcampSerializationSolutionWorkflow() throws Exception {
        testWorkflow(new BootcampSerializationSolutionWorkflow());
    }
}
