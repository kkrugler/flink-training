package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.provided.CurrencyRateAPI;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.common.ECommerceTestUtils.createShoppingCart;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class ECommerceEnrichmentSolution3WorkflowTest {

    // The beginning time for our workflow, for events
    private static final long START_TIME = 0;

    @Test
    public void testAddingUSDEquivalent() throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism 2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);

        List<ShoppingCartRecord> records = new ArrayList<>();
        ShoppingCartGenerator generator = new ShoppingCartGenerator(START_TIME);

        // Create a completed record, with two items.
        ShoppingCartRecord r1 = createShoppingCart(generator, "US");
        CartItem c11 = r1.getItems().get(0);
        c11.setQuantity(2);
        r1.getItems().add(generator.createCartItem("US"));
        CartItem c12 = r1.getItems().get(1);
        c12.setQuantity(1);
        r1.setTransactionTime(START_TIME + 2000);
        r1.setTransactionCompleted(true);
        records.add(r1);

        // Create a completed record, in a future window - since our windows
        // are in 1 minute intervals (tumbling).
        ShoppingCartRecord r2 = createShoppingCart(generator, "US");
        CartItem c21 = r2.getItems().get(0);
        c21.setQuantity(1);
        r2.setTransactionTime(START_TIME + Duration.ofMinutes(5).toMillis());
        r2.setTransactionCompleted(true);
        records.add(r2);

        // Create a completed record for a different country
        ShoppingCartRecord r3 = createShoppingCart(generator, "MX");
        CartItem c31 = r3.getItems().get(0);
        c31.setQuantity(10);
        r3.setTransactionTime(START_TIME + 0);
        r3.setTransactionCompleted(true);
        records.add(r3);

        ResultsSink sink = new ResultsSink();

        // Set starting time for calls to currency convertor
        // So it can be set to 0.
        new ECommerceEnrichmentSolution3Workflow()
                .setCartStream(env.fromData(records).setParallelism(1))
                .setResultSink(sink)
                // Set up start time used by currency exchange rate API
                .setStartTime(START_TIME)
                .build();

        env.execute("ECommerceWindowingSolution1Job");

        // Validate we get the expected results.
        assertThat(sink.getSink()).containsExactlyInAnyOrder(
                Tuple3.of("US",START_TIME + Duration.ofMinutes(0).toMillis(), estimateSpend(r1, Duration.ofMinutes(0))),
                Tuple3.of("US",START_TIME + Duration.ofMinutes(5).toMillis(), estimateSpend(r2, Duration.ofMinutes(5))),
                Tuple3.of("MX", START_TIME + Duration.ofMinutes(0).toMillis(), estimateSpend(r3, Duration.ofMinutes(0)))
        );

    }

    private double estimateSpend(ShoppingCartRecord cartRecord, Duration currentRateTime) {
        String country = cartRecord.getCountry();
        CurrencyRateAPI api = new CurrencyRateAPI(0);
        double rate = api.getRate(country, currentRateTime);
        double result = 0.0;
        for (CartItem item : cartRecord.getItems()) {
            result += (item.getPrice() * rate) * item.getQuantity();
        }

        return result;
    }

    private static class ResultsSink extends MockSink<Tuple3<String, Long, Double>> {

        private static final ConcurrentLinkedQueue<Tuple3<String, Long, Double>> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<Tuple3<String, Long, Double>> getSink() {
            return QUEUE;
        }
    }

}