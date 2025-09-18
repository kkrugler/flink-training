/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.solutions;

import com.ververica.flink.training.exercises.BootcampStateQueue;
import org.junit.jupiter.api.Test;

import java.util.function.BiFunction;

import static com.ververica.flink.training.provided.BootcampStateQueueTestUtils.*;

class BootcampStateSolutionQueueTest {

    @Test
    public void testQueueSingleEntry() throws Exception {
        testQueueSingleEntryUtil(new QueueFactory());
    }

    @Test
    public void testQueueAsBatcher() throws Exception {
        testQueueAsBatcherUtil(new QueueFactory());
    }

    private static class QueueFactory implements BiFunction<String, Class<?>, BootcampStateQueue> {

        @Override
        public BootcampStateQueue apply(String prefix, Class<?> queueType) {
            return new BootcampStateSolutionQueue<>(prefix, queueType);
        }
    }
}
