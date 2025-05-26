/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.exercises;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampReviewWorkflowTestUtils.testReview1Workflow;

public class BootcampReview1WorkflowTest {

    @Test
    public void testBootcampReview1Workflow() throws Exception {
        testReview1Workflow(new BootcampReview1Workflow());
    }
}
