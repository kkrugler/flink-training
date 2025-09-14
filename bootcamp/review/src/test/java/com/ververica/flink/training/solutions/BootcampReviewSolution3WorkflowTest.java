/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampReviewWorkflowTestUtils.testReview3Workflow;

class BootcampReviewSolution3WorkflowTest {

    @Test
    public void testBootcampReviewSolution3Workflow() throws Exception {
        testReview3Workflow(new BootcampReviewSolution3Workflow());
    }
}
