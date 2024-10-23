package com.ververica.flink.training.solutions;

import com.ververica.flink.training.exercises.BootcampStateQueue;
import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampStateQueueTestUtils.*;

import java.util.function.BiFunction;


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