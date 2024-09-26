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

class ECommerceEnrichmentSolution1WorkflowTest {

    @Test
    public void testAddingUSDEquivalent() throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism 2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);

        List<ShoppingCartRecord> records = new ArrayList<>();
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);


        // Create a completed record, with two items.
        ShoppingCartRecord r1 = createShoppingCart(generator, "US");
        CartItem c11 = r1.getItems().get(0);
        c11.setQuantity(2);
        r1.getItems().add(generator.createCartItem("US"));
        CartItem c12 = r1.getItems().get(1);
        c12.setQuantity(1);
        r1.setTransactionTime(2000);
        r1.setTransactionCompleted(true);
        records.add(r1);

        // Create a completed record, in a future window - since our windows
        // are in 1 minute intervals (tumbling).
        ShoppingCartRecord r2 = createShoppingCart(generator, "US");
        CartItem c21 = r2.getItems().get(0);
        c21.setQuantity(1);
        r2.setTransactionTime(Duration.ofMinutes(5).toMillis());
        r2.setTransactionCompleted(true);
        records.add(r2);

        // Create a completed record for a different country
        ShoppingCartRecord r3 = createShoppingCart(generator, "MX");
        CartItem c31 = r3.getItems().get(0);
        c31.setQuantity(10);
        r3.setTransactionTime(0);
        r3.setTransactionCompleted(true);
        records.add(r3);

        ResultsSink sink = new ResultsSink();

        // Set starting time for calls to currency convertor
        // So it can be set to 0.
        new ECommerceEnrichmentSolution1Workflow()
                .setCartStream(env.fromData(records).setParallelism(1))
                .setResultSink(sink)
                // Set up start time used by currency exchange rate API
                .setStartTime(0)
                .build();

        env.execute("ECommerceWindowingSolution1Job");

        // Validate we get the expected results. For the two entries in the first window,
        // we know the exchange rate and thus can do an exact comparison
        assertThat(sink.getSink()).contains(
                Tuple3.of("US",Duration.ofMinutes(0).toMillis(), estimateSpend(r1)),
                Tuple3.of("MX", Duration.ofMinutes(0).toMillis(), estimateSpend(r3))
        );

        // TODO validate we have 3 results, and there's one for US that's close to what
        // we need.
        // Tuple3.of("US", Duration.ofMinutes(5).toMillis(), 1),


    }

    private double estimateSpend(ShoppingCartRecord cartRecord) {
        String country = cartRecord.getCountry();
        double rate = country.equals("US") ? 1.0 : ExchangeRates.STARTING_RATES.get(country);
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