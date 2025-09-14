/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.exercises;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampReviewWorkflowTestUtils.testReview4Workflow;

public class BootcampReview4WorkflowTest {

    @Test
    public void testBootcampReview4Workflow() throws Exception {
        testReview4Workflow(new BootcampReview4Workflow());
    }
}
