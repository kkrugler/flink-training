package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.ProductInfoGenerator;
import com.ververica.flink.training.common.ProductInfoRecord;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Add shipping cost to eCommerce records using the ProductInfoGenerator to add weight,
 * and then estimate shipping cost using that weight.
 */
public class EnrichWithShippingCost extends RichMapFunction<ShoppingCartRecord, ShoppingCartRecord> {

    // Map from productId to ProductInfoRecord.
    private transient Map<String, ProductInfoRecord> products;

    @Override
    public void open(OpenContext openContext) throws Exception {
        products = new HashMap<>();

        ProductInfoGenerator productGenerator = new ProductInfoGenerator();
        for (long i = 0; i < ProductInfoGenerator.NUM_UNIQUE_PRODUCTS; i++) {
            ProductInfoRecord pir = productGenerator.apply(i);
            products.put(pir.getProductId(), pir);
        }
    }

    @Override
    public ShoppingCartRecord map(ShoppingCartRecord value) throws Exception {
        double totalWeight = 0.0;
        for (CartItem item : value.getItems()) {
            ProductInfoRecord pir = products.get(item.getProductId());
            if (pir == null) {
                throw new NoSuchElementException();
            }

            double weight = pir.getWeightKg();
            item.setWeightKg(weight);
            totalWeight += (weight * item.getQuantity());
        }

        // Cost is weight * costPerKg
        value.setShippingCost(totalWeight * 0.73);
        return value;
    }
}
