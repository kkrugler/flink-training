package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.provided.TrimmedShoppingCart;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.ververica.flink.training.common.BootcampTestUtils.*;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class BootcampTablesSolutionWorkflowTest {

    // The beginning time for our workflow, for events
    private static final long START_TIME = 0;

    @Test
    public void testWorkflow() throws Exception {
        List<ShoppingCartRecord> fullCarts = BootcampTestUtils.makeCartRecords();
        List<TrimmedShoppingCart> trimmedCarts = new ArrayList<>();
        fullCarts.forEach(r -> trimmedCarts.add(new TrimmedShoppingCart(r)));

        ResultsSink sink = new ResultsSink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        new BootcampTablesSolutionWorkflow(env)
                .setCartStream(env.fromData(trimmedCarts).setParallelism(1))
                .setExchangeRateStream(null /* todo set this up */)
                .setResultsSink(sink)
                .build();

        env.execute("BootcampEnrichment1Workflow");

        // Validate we get the expected results.
        assertThat(sink.getSink()).containsExactlyInAnyOrder(
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(0).toMillis(),
                        estimateSpend(fullCarts, "r3", Duration.ofMinutes(0))),
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(5).toMillis(),
                        estimateSpend(fullCarts, "r4", Duration.ofMinutes(5))),
                new KeyedWindowDouble("MX", START_TIME + Duration.ofMinutes(0).toMillis(),
                        estimateSpend(fullCarts, "r5", Duration.ofMinutes(0)))
        );
    }

    private static class ResultsSink extends MockSink<TrimmedShoppingCart> {

        private static final ConcurrentLinkedQueue<TrimmedShoppingCart> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<TrimmedShoppingCart> getSink() {
            return QUEUE;
        }
    }

}