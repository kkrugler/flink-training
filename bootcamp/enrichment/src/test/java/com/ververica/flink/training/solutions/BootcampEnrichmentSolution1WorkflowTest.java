package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.provided.CurrencyRateAPI;
import com.ververica.flink.training.provided.KeyedWindowDouble;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class BootcampEnrichmentSolution1WorkflowTest {

    // The beginning time for our workflow, for events
    private static final long START_TIME = 0;

    @Test
    public void testAddingUSDEquivalent() throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        ResultsSink sink = new ResultsSink();

        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism", "2"});
        final StreamExecutionEnvironment env = EnvironmentUtils.createConfiguredEnvironment(parameters);
        new BootcampEnrichmentSolution1Workflow()
                .setCartStream(env.fromData(records).setParallelism(1))
                .setResultSink(sink)
                // Set up start time used by currency exchange rate API
                .setStartTime(START_TIME)
                .build();

        env.execute("BootcampEnrichmentSolution1Job");

        // Validate we get the expected results.
        assertThat(sink.getSink()).containsExactlyInAnyOrder(
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(0).toMillis(),
                        estimateSpend(records, "r3", Duration.ofMinutes(0))),
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(5).toMillis(),
                        estimateSpend(records, "r4", Duration.ofMinutes(5))),
                new KeyedWindowDouble("MX", START_TIME + Duration.ofMinutes(0).toMillis(),
                        estimateSpend(records, "r5", Duration.ofMinutes(0)))
        );
    }

    private double estimateSpend(List<ShoppingCartRecord> records, String transactionId, Duration currentRateTime) {
        ShoppingCartRecord cartRecord = null;
        for (ShoppingCartRecord cart : records) {
            if (cart.isTransactionCompleted() && (cart.getTransactionId().equals(transactionId))) {
                cartRecord = cart;
                break;
            }
        }
        assertThat(cartRecord).isNotNull();

        String country = cartRecord.getCountry();
        CurrencyRateAPI api = new CurrencyRateAPI(START_TIME);
        double rate = api.getRate(country, currentRateTime);
        double result = 0.0;
        for (CartItem item : cartRecord.getItems()) {
            result += (item.getPrice() * rate) * item.getQuantity();
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