/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampReviewWorkflowTestUtils.testReview4Workflow;

class BootcampReviewSolution4WorkflowTest {

    @Test
    public void testBootcampReviewSolution4Workflow() throws Exception {
        testReview4Workflow(new BootcampReviewSolution4Workflow());
    }
}
