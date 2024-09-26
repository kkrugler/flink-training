package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static com.ververica.flink.training.common.ECommerceTestUtils.*;

class ECommerceWindowingSolution1WorkflowTest {

    @Test
    public void testAggregation() throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism 2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);

        List<ShoppingCartRecord> records = new ArrayList<>();
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);

        // Add a record that should get ignored (not completed)
        ShoppingCartRecord r1 = createShoppingCart(generator, "US");
        records.add(r1);

        // Add an updated entry. This should also get ignored in analysis, since it's
        // not completed.
        ShoppingCartRecord r2 = createShoppingCart(generator, "US");
        r2.setTransactionTime(1000);
        records.add(r2);

        // Create a completed record, with two items.
        ShoppingCartRecord r3 = createShoppingCart(generator, "US");
        CartItem c31 = r3.getItems().get(0);
        c31.setQuantity(2);
        r3.getItems().add(generator.createCartItem("US"));
        CartItem c32 = r3.getItems().get(1);
        c32.setQuantity(1);
        r3.setTransactionTime(2000);
        r3.setTransactionCompleted(true);
        records.add(r3);

        // Create a completed record, in a future window - since our windows
        // are in 1 minute intervals (tumbling).
        ShoppingCartRecord r4 = createShoppingCart(generator, "US");
        CartItem c41 = r4.getItems().get(0);
        c41.setQuantity(1);
        r4.setTransactionTime(Duration.ofMinutes(5).toMillis());
        r4.setTransactionCompleted(true);
        records.add(r4);

        // Create a completed record for a different country
        ShoppingCartRecord r5 = createShoppingCart(generator, "MX");
        CartItem c51 = r5.getItems().get(0);
        c51.setQuantity(10);
        r5.setTransactionTime(0);
        r5.setTransactionCompleted(true);
        records.add(r5);

        ResultsSink sink = new ResultsSink();

        new ECommerceWindowingSolution1Workflow()
                .setCartStream(env.fromData(records).setParallelism(1))
                .setResultSink(sink)
                .build();

        env.execute("ECommerceWindowingSolution1Job");

        // Validate we get the expected results.
        assertThat(sink.getSink()).containsExactlyInAnyOrder(
                Tuple3.of("US",Duration.ofMinutes(0).toMillis(), 3),
                Tuple3.of("US", Duration.ofMinutes(5).toMillis(), 1),
                Tuple3.of("MX", Duration.ofMinutes(0).toMillis(), 10)
        );

    }

    private static class ResultsSink extends MockSink<Tuple3<String, Long, Integer>> {

        private static final ConcurrentLinkedQueue<Tuple3<String, Long, Integer>> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<Tuple3<String, Long, Integer>> getSink() {
            return QUEUE;
        }
    }
}