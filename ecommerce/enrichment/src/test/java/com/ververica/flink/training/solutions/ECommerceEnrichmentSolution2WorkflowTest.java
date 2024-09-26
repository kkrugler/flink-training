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

class ECommerceEnrichmentSolution2WorkflowTest {

    private static final long START_TIME = 0;

    @Test
    public void testAddingProductWeight() throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism 2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);

        List<ShoppingCartRecord> carts = new ArrayList<>();
        ShoppingCartGenerator cartGenerator = new ShoppingCartGenerator(START_TIME);

        // Create a completed record, with two items.
        ShoppingCartRecord r1 = createShoppingCart(cartGenerator, "US");
        CartItem c11 = r1.getItems().get(0);
        c11.setQuantity(2);
        r1.getItems().add(cartGenerator.createCartItem("US"));
        CartItem c12 = r1.getItems().get(1);
        c12.setQuantity(1);
        r1.setTransactionTime(START_TIME + 2000);
        r1.setTransactionCompleted(true);
        carts.add(r1);

        // Create a completed record, in a future window - since our windows
        // are in 1 minute intervals (tumbling).
        ShoppingCartRecord r2 = createShoppingCart(cartGenerator, "US");
        CartItem c21 = r2.getItems().get(0);
        c21.setQuantity(1);
        r2.setTransactionTime(START_TIME + Duration.ofMinutes(5).toMillis());
        r2.setTransactionCompleted(true);
        carts.add(r2);

        // Create a completed record for a different country
        ShoppingCartRecord r3 = createShoppingCart(cartGenerator, "MX");
        CartItem c31 = r3.getItems().get(0);
        c31.setQuantity(10);
        r3.setTransactionTime(START_TIME + 0);
        r3.setTransactionCompleted(true);
        carts.add(r3);

        // Generate product info for every product.
        List<ProductInfoRecord> products = new ArrayList<>();
        ProductInfoGenerator productGenerator = new ProductInfoGenerator();
        for (long i = 0; i < ProductInfoGenerator.NUM_UNIQUE_PRODUCTS; i++) {
            products.add(productGenerator.apply(i));
        }

        ResultsSink sink = new ResultsSink();

        new ECommerceEnrichmentSolution2Workflow()
                .setCartStream(env.fromData(carts).setParallelism(1))
                .setProductInfoStream(env.fromData(products).setParallelism(1))
                .setResultSink(sink)
                .build();

        env.execute("ECommerceWindowingSolution1Job");

        // Validate we get the expected results.
        assertThat(sink.getSink()).containsExactlyInAnyOrder(
                Tuple3.of("US",START_TIME + Duration.ofMinutes(0).toMillis(), calcTotalWeight(productGenerator, r1)),
                Tuple3.of("US",START_TIME + Duration.ofMinutes(5).toMillis(), calcTotalWeight(productGenerator, r2)),
                Tuple3.of("MX", START_TIME + Duration.ofMinutes(0).toMillis(), calcTotalWeight(productGenerator, r3))
        );

    }

    private double calcTotalWeight(ProductInfoGenerator productGenerator, ShoppingCartRecord cartRecord) {
        double result = 0.0;
        for (CartItem item : cartRecord.getItems()) {
            result += (productGenerator.getProductWeight(item.getProductId()) * item.getQuantity());
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