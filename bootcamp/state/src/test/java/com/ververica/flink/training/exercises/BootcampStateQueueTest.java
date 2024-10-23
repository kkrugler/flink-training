package com.ververica.flink.training.exercises;

import org.junit.jupiter.api.Test;

import java.util.function.BiFunction;

import static com.ververica.flink.training.provided.BootcampStateQueueTestUtils.testQueueAsBatcherUtil;
import static com.ververica.flink.training.provided.BootcampStateQueueTestUtils.testQueueSingleEntryUtil;


class BootcampStateQueueTest {

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
            return new BootcampStateQueue<>(prefix, queueType);
        }
    }
}