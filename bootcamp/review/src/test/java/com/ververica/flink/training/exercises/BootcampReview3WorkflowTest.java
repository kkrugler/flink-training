/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.exercises;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampReviewWorkflowTestUtils.testReview3Workflow;

public class BootcampReview3WorkflowTest {

    @Test
    public void testBootcampReview3Workflow() throws Exception {
        testReview3Workflow(new BootcampReview3Workflow());
    }
}
