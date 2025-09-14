/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampReviewWorkflowTestUtils.testReview1Workflow;
import static com.ververica.flink.training.provided.BootcampReviewWorkflowTestUtils.testReview2Workflow;

class BootcampReviewSolution2WorkflowTest {

    @Test
    public void testBootcampReviewSolution2Workflow() throws Exception {
        testReview2Workflow(new BootcampReviewSolution2Workflow());
    }
}
