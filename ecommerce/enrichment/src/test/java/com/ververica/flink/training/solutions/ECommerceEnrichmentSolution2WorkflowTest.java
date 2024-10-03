package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.provided.KeyedWindowDouble;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class ECommerceEnrichmentSolution2WorkflowTest {

    private static final long START_TIME = 0;

    @Test
    public void testAddingProductWeight() throws Exception {
        List<ShoppingCartRecord> carts = ECommerceTestUtils.makeCartRecords();

        // Generate product info for every product.
        List<ProductInfoRecord> products = new ArrayList<>();
        ProductInfoGenerator productGenerator = new ProductInfoGenerator();
        for (long i = 0; i < ProductInfoGenerator.NUM_UNIQUE_PRODUCTS; i++) {
            products.add(productGenerator.apply(i));
        }

        ResultsSink sink = new ResultsSink();

        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism", "2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);
        new ECommerceEnrichmentSolution2Workflow()
                .setCartStream(env.fromData(carts).setParallelism(1))
                .setProductInfoStream(env.fromData(products).setParallelism(1))
                .setResultSink(sink)
                .build();

        env.execute("ECommerceWindowingSolution2Job");

        // Validate we get the expected results.
        assertThat(sink.getSink()).containsExactlyInAnyOrder(
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(0).toMillis(),
                        calcTotalWeight(carts, "r3", productGenerator)),
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(5).toMillis(),
                        calcTotalWeight(carts, "r4", productGenerator)),
                new KeyedWindowDouble("MX", START_TIME + Duration.ofMinutes(0).toMillis(),
                        calcTotalWeight(carts, "r5", productGenerator))
        );

    }

    private double calcTotalWeight(List<ShoppingCartRecord> records, String transactionId, ProductInfoGenerator productGenerator) {
        ShoppingCartRecord cartRecord = null;
        for (ShoppingCartRecord cart : records) {
            if (cart.isTransactionCompleted() && (cart.getTransactionId().equals(transactionId))) {
                cartRecord = cart;
                break;
            }
        }
        assertThat(cartRecord).isNotNull();

        double result = 0.0;
        for (CartItem item : cartRecord.getItems()) {
            result += (productGenerator.getProductWeight(item.getProductId()) * item.getQuantity());
        }

        return result;
    }

    private static class ResultsSink extends MockSink<KeyedWindowDouble> {

        private static final ConcurrentLinkedQueue<KeyedWindowDouble> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowDouble> getSink() {
            return QUEUE;
        }
    }

}