package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.ShoppingCartGenerator;
import com.ververica.flink.training.provided.ECommerceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class BatchedCartsTest {

    @Test
    public void testBuildAndIterator() throws Exception {
        BatchedCarts.Builder builder = new BatchedCarts.Builder(new ReportByCountrySortByShippingCost());
        assertEquals(0, builder.getNumCarts());

        long recordIndex = 0;
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);
        ECommerceRecord r1 = new ECommerceRecord(generator.apply(recordIndex++));
        r1.setCountry("US");
        builder.add(r1);
        assertEquals(1, builder.getNumCarts());

        // Should fail with different country
        ECommerceRecord failed = new ECommerceRecord(generator.apply(recordIndex++));
        failed.setCountry("MX");
        try {
            builder.add(failed);
            fail("Should have thrown exception with different country");
        } catch (Exception e) {

        }
        assertEquals(1, builder.getNumCarts());

        // Add a second record.
        ECommerceRecord r2 = new ECommerceRecord(generator.apply(recordIndex++));
        r2.setCountry("US");
        builder.add(r2);
        assertEquals(2, builder.getNumCarts());

        BatchedCarts batched = builder.build();
        assertEquals("US", batched.getKey());
        assertEquals(2, batched.size());

        List<ECommerceRecord> results = new ArrayList<>();
        batched.iterator().forEachRemaining(r -> results.add(r));
        assertThat(results).containsExactlyInAnyOrder(
                r1,
                r2
        );

    }

}