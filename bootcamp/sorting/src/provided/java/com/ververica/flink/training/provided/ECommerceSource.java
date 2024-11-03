package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class ECommerceSource extends FakeParallelSource<ECommerceRecord> {
    public ECommerceSource(long numRecords) {
        super(numRecords, 0L, true, getECommerceGenerator());
    }

    public ECommerceSource(long numRecords, long delay, boolean bounded) {
        super(numRecords, delay, bounded, getECommerceGenerator());
    }

    private static SerializableFunction<Long, ECommerceRecord> getECommerceGenerator() {
        // Set starting time to be 10 days ago
        return new ECommerceGenerator(System.currentTimeMillis() - Duration.ofDays(10).toMillis());
    }

    private static class ECommerceGenerator implements SerializableFunction<Long, ECommerceRecord> {

        private long startingTime;

        private transient ShoppingCartGenerator generator;
        private transient Map<String, ProductInfoRecord> products;

        public ECommerceGenerator(long startingTime) {
            this.startingTime = startingTime;
        }

        @Override
        public ECommerceRecord apply(Long recordIndex) {
            init();

            ShoppingCartRecord scr = generator.apply(recordIndex);

            double totalWeight = 0.0;
            for (CartItem item : scr.getItems()) {
                ProductInfoRecord pir = products.get(item.getProductId());
                if (pir == null) {
                    throw new NoSuchElementException();
                }

                double weight = pir.getWeightKg();
                item.setWeightKg(weight);
                totalWeight += (weight * item.getQuantity());
            }

            // Cost is weight * costPerKg
            scr.setShippingCost(totalWeight * 0.73);

            return new ECommerceRecord(scr);
        }

        private void init() {
            if (generator == null) {
                generator = new ShoppingCartGenerator(startingTime);
            }

            if (products == null) {
                products = new HashMap<>();

                ProductInfoGenerator productGenerator = new ProductInfoGenerator();
                for (long i = 0; i < ProductInfoGenerator.NUM_UNIQUE_PRODUCTS; i++) {
                    ProductInfoRecord pir = productGenerator.apply(i);
                    products.put(pir.getProductId(), pir);
                }
            }
        }
    }

}
