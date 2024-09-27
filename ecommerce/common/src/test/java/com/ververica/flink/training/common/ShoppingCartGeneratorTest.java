package com.ververica.flink.training.common;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ShoppingCartGeneratorTest {

    @Test
    public void testGeneratingCompletedTransactions() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);

        int numCompleted = 0;
        for (int i = 0; i < 10000; i++) {
            ShoppingCartRecord r = generator.apply((long)i);
            if (r.isTransactionCompleted()) {
                numCompleted++;
            }
        }

        assertTrue(numCompleted > 0, "Must have some completed");
    }

    @Test
    public void testGeneratingUpdatedTransactions() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);

        int numUpdates = 0;
        Set<String> pendingTransactions = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            ShoppingCartRecord r = generator.apply((long)i);
            if (pendingTransactions.add(r.getTransactionId())) {
                numUpdates++;
            }
        }

        assertTrue(numUpdates > 0, "Must have some updates");
    }
    @Test
    public void testNotUpdatingCompletedTransactions() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);

        Set<String> completedTransactions = new HashSet<>();
        for (int i = 0; i < 100_000; i++) {
            ShoppingCartRecord r = generator.apply((long)i);
            if (r.isTransactionCompleted()) {
                assertTrue(completedTransactions.add(r.getTransactionId()));
            } else {
                assertFalse(completedTransactions.contains(r.getTransactionId()));
            }
        }
    }

    @Test
    public void testUniqueProducts() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);
        Set<String> productIds = new HashSet<>();
        for (long i = 0; i < 100_000; i++) {
            ShoppingCartRecord r = generator.apply(i);
            for (CartItem item : r.getItems()) {
                productIds.add(item.getProductId());
            }
        }

        assertEquals(ProductInfoGenerator.NUM_UNIQUE_PRODUCTS, productIds.size());
    }

    @Test
    public void testProductQuantity() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);
        Set<Integer> productQuantities = new HashSet<>();
        for (long i = 0; i < 100_000; i++) {
            ShoppingCartRecord r = generator.apply(i);
            for (CartItem item : r.getItems()) {
                assertTrue(item.getQuantity() >= 1);
                assertTrue(item.getQuantity() <= ShoppingCartGenerator.MAX_PRODUCT_QUANTITY);

                productQuantities.add(item.getQuantity());
            }
        }

        assertEquals(ShoppingCartGenerator.MAX_PRODUCT_QUANTITY, productQuantities.size());
    }

    // Verify that we get the same shipping address for the same customer id
    @Test
    public void testFakeAddresses() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);
        Map<String, String> addresses = new HashMap<>();

        for (long i = 0; i < 100_000; i++) {
            ShoppingCartRecord r = generator.apply(i);
            String customerId = r.getCustomerId();
            String shippinAddress = r.getShippingAddress();
            assertNotNull(shippinAddress);
            assertNotEquals("", shippinAddress);

            if (addresses.containsKey(customerId)) {
                assertEquals(addresses.get(customerId), shippinAddress);
            } else {
                addresses.put(customerId, shippinAddress);
            }
        }
    }
}