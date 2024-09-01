package com.ververica.flink.training.common;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ShoppingCartGeneratorTest {

    @Test
    public void testGeneratingCompletedTransactions() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);

        int numCompleted = 0;
        for (int i = 0; i < 10000; i++) {
            ShoppingCartRecord r = (ShoppingCartRecord)generator.apply((long)i);
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
            ShoppingCartRecord r = (ShoppingCartRecord)generator.apply((long)i);
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
            ShoppingCartRecord r = (ShoppingCartRecord)generator.apply((long)i);
            if (r.isTransactionCompleted()) {
                assertTrue(completedTransactions.add(r.getTransactionId()));
            } else {
                assertFalse(completedTransactions.contains(r.getTransactionId()));
            }
        }
    }
}