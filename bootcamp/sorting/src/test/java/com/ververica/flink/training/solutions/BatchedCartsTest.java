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
        ReportBy reportBy = new ReportByCountrySortByShippingCost();
        BatchedCarts.Builder builder = new BatchedCarts.Builder(reportBy);
        assertEquals(0, builder.getNumCarts());

        long recordIndex = 0;
        ShoppingCartGenerator generator = new ShoppingCartGenerator(0);
        ECommerceRecord r1 = new ECommerceRecord(generator.apply(recordIndex++));
        r1.setCountry("US");
        builder.add(r1);
        assertEquals(1, builder.getNumCarts());

        // Add a second record.
        ECommerceRecord r2 = new ECommerceRecord(generator.apply(recordIndex++));
        r2.setCountry("US");
        builder.add(r2);
        assertEquals(2, builder.getNumCarts());

        BatchedCarts batched = builder.build();
        assertEquals(2, batched.size());

        List<ReportByRecord> results = new ArrayList<>();
        batched.iterator().forEachRemaining(r -> results.add(r));
        assertThat(results).containsExactlyInAnyOrder(
                reportBy.getSortableRecord(r1),
                reportBy.getSortableRecord(r2)
        );

    }

}