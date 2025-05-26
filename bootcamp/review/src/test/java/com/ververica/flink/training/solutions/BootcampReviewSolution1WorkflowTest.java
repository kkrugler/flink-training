/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampReviewWorkflowTestUtils.testReview1Workflow;

class BootcampReviewSolution1WorkflowTest {

    @Test
    public void testBootcampReviewSolution1Workflow() throws Exception {
        testReview1Workflow(new BootcampReviewSolution1Workflow());
    }
}
