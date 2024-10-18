package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.provided.TrimmedShoppingCart;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.ververica.flink.training.common.BootcampTestUtils.*;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class BootcampTablesSolutionWorkflowTest {

    // The beginning time for our workflow, for events
    private static final long START_TIME = 0;

    private static final Date START_DATE = new Date(START_TIME);

    @Test
    // FIXME - reenable
    @Disabled
    public void testWorkflow() throws Exception {
        List<ShoppingCartRecord> fullCarts = BootcampTestUtils.makeCartRecords();
        List<TrimmedShoppingCart> trimmedCarts = new ArrayList<>();
        fullCarts.forEach(r -> trimmedCarts.add(new TrimmedShoppingCart(r)));

        List<CurrencyConversionRecord> exchangeRates = new ArrayList<>();
        for (String country : ExchangeRates.STARTING_RATES.keySet()) {
            exchangeRates.add(new CurrencyConversionRecord(country, START_DATE,
                    ExchangeRates.STARTING_RATES.get(country)));
        }

        ResultsSink sink = new ResultsSink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        new BootcampTablesSolutionWorkflow(env)
                .setCartStream(env.fromData(trimmedCarts).setParallelism(1))
                .setExchangeRateStream(env.fromData(exchangeRates))
                .setResultsSink(sink)
                .build();

        // FIXME - table job termination w/batch sources
        JobClient client = env.executeAsync("BootcampTables1Workflow");

        Thread.sleep(2000L);

        client.cancel();

        // Validate we get the expected results.
        assertThat(sink.getSink()).containsExactlyInAnyOrder(
                makeResultCart(trimmedCarts, "r3"),
                makeResultCart(trimmedCarts,"r4"),
                makeResultCart(trimmedCarts,"r5")
        );
    }

    private TrimmedShoppingCart makeResultCart(List<TrimmedShoppingCart> trimmedCarts, String transactionId) {
        // Find the cart by transactionId.
        TrimmedShoppingCart cartRecord = null;
        for (TrimmedShoppingCart cart : trimmedCarts) {
            if (cart.isTransactionCompleted() && (cart.getTransactionId().equals(transactionId))) {
                cartRecord = cart;
                break;
            }
        }
        assertThat(cartRecord).isNotNull();

        String country = cartRecord.getCountry();
        CurrencyRateAPI api = new CurrencyRateAPI(START_TIME);
        double rate = api.getRate(country, cartRecord.getTransactionTime() - START_TIME);
        double result = 0.0;
        for (CartItem item : cartRecord.getItems()) {
            item.setUsDollarEquivalent(item.getPrice() * rate);
        }

        return cartRecord;
    }

    private static class ResultsSink extends MockSink<TrimmedShoppingCart> {

        private static final ConcurrentLinkedQueue<TrimmedShoppingCart> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<TrimmedShoppingCart> getSink() {
            return QUEUE;
        }
    }

}