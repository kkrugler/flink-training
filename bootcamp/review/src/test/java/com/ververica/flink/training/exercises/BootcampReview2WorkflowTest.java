/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.exercises;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampReviewWorkflowTestUtils.testReview2Workflow;

public class BootcampReview2WorkflowTest {

    @Test
    public void testBootcampReview2Workflow() throws Exception {
        testReview2Workflow(new BootcampReview2Workflow());
    }
}
