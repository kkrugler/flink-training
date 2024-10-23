package com.ververica.flink.training.provided;

import com.ververica.flink.training.exercises.BootcampStateQueue;
import com.ververica.flink.training.solutions.BootcampStateSolutionQueue;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BootcampStateQueueTestUtils {

    public static void testQueueSingleEntryUtil(BiFunction<String, Class<?>, BootcampStateQueue> queueFactory) throws Exception {

        TestQueueFunction function = new TestQueueFunction(queueFactory);
        StreamFlatMap<Tuple2<String, Long>, Long> operator = new StreamFlatMap<>(function);
        KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, Long>, Long> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                operator, t -> t.f0, TypeInformation.of(String.class));
        harness.open();

        harness.processElement(Tuple2.of("a", 0L), 0);
        assertEquals(1,  harness.getRecordOutput().size());
        harness.processElement(Tuple2.of("a", 0L), 0);
        assertEquals(1,  harness.getRecordOutput().size());
        harness.processElement(Tuple2.of("a", 0L), 0);
        assertEquals(1,  harness.getRecordOutput().size());
        harness.processElement(Tuple2.of("a", 1L), 0);
        assertEquals(2,  harness.getRecordOutput().size());

        // Add record with a different key, which will have different state.
        harness.processElement(Tuple2.of("b", 0L), 0);
        assertEquals(3,  harness.getRecordOutput().size());
        harness.processElement(Tuple2.of("b", 0L), 0);
        assertEquals(3,  harness.getRecordOutput().size());

        harness.close();
    }

    public static void testQueueAsBatcherUtil(BiFunction<String, Class<?>, BootcampStateQueue> queueFactory) throws Exception {

        final int batchSize = 3;
        TestBatchFunction function = new TestBatchFunction(queueFactory, batchSize);
        StreamFlatMap<Tuple2<String, Long>, Long> operator = new StreamFlatMap<>(function);
        KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, Long>, Long> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, t -> t.f0, TypeInformation.of(String.class));
        harness.open();

        // First two records get batched.
        harness.processElement(Tuple2.of("a", 1L), 0);
        assertEquals(0,  harness.getRecordOutput().size());
        harness.processElement(Tuple2.of("a", 2L), 0);
        assertEquals(0,  harness.getRecordOutput().size());

        // Third record flushes the batch
        harness.processElement(Tuple2.of("a", 3L), 0);
        assertEquals(3,  harness.getRecordOutput().size());

        // Next two records are again batched.
        harness.processElement(Tuple2.of("a", 4L), 0);
        assertEquals(3,  harness.getRecordOutput().size());
        harness.processElement(Tuple2.of("a", 5L), 0);
        assertEquals(3,  harness.getRecordOutput().size());

        // Third record once again flushes the batch.
        harness.processElement(Tuple2.of("a", 6L), 0);
        assertEquals(6,  harness.getRecordOutput().size());

        // Verify what's dumped exactly matches what we were batching, in the
        // same order.
        assertThat(harness.getRecordOutput()).containsExactly(
                new StreamRecord<Long>(1L, 0),
                new StreamRecord<Long>(2L, 0),
                new StreamRecord<Long>(3L, 0),
                new StreamRecord<Long>(4L, 0),
                new StreamRecord<Long>(5L, 0),
                new StreamRecord<Long>(6L, 0)
        );

        harness.close();
    }

    private static class TestQueueFunction extends RichFlatMapFunction<Tuple2<String, Long>, Long> {

        private BiFunction<String, Class<?>, BootcampStateQueue> queueFactory;

        transient BootcampStateQueue<Long> testQueue;

        public TestQueueFunction(BiFunction<String, Class<?>, BootcampStateQueue> queueFactory) {
            this.queueFactory = queueFactory;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            testQueue = queueFactory.apply("TestQueueFunction", Long.class);
            testQueue.open(getRuntimeContext());
        }

        @Override
        public void flatMap(Tuple2<String, Long> in, Collector<Long> out) throws Exception {
            Long lastValue = testQueue.peek();
            if ((lastValue != null) && (lastValue == in.f1)) {
                // Ignore the record
            } else {
                assertTrue(testQueue.offer(in.f1));
                out.collect(in.f1);
            }
        }


        @Override
        public void close() throws Exception {
            super.close();
        }

    }

    private static class TestBatchFunction extends RichFlatMapFunction<Tuple2<String, Long>, Long> {

        private final BiFunction<String, Class<?>, BootcampStateQueue> queueFactory;
        private final int batchSize;

        transient BootcampStateQueue<Long> testQueue;

        public TestBatchFunction(BiFunction<String, Class<?>, BootcampStateQueue> queueFactory, int batchSize) {
            this.queueFactory = queueFactory;
            this.batchSize = batchSize;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            testQueue = queueFactory.apply("TestQueueFunction", Long.class);
            testQueue.open(getRuntimeContext());
        }

        @Override
        public void flatMap(Tuple2<String, Long> in, Collector<Long> out) throws Exception {
            testQueue.add(in.f1);
            if (testQueue.size() >= batchSize) {
                testQueue.iterator().forEachRemaining(r -> out.collect(r));
                testQueue.clear();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

    }
}