package com.ververica.flink.training.common;

import org.apache.flink.annotation.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ShoppingCartGenerator implements SerializableFunction<Long, ShoppingCartRecord> {

    private static final int MAX_ACTIVE_CARTS = 100;

    private static final long PER_TRANSACTION_GAP_MS = 10;

    public static final String[] COUNTRIES = new String[]{
            "US",
            "JP",
            "CN",
            "CA",
            "MX"
    };

    private long curTime;

    private final Random rand = new Random();
    private List<ShoppingCartRecord> activeCarts = new ArrayList<>();

    public ShoppingCartGenerator(long startingTime) {
        this.curTime = startingTime;

    }

    @Override
    public ShoppingCartRecord apply(Long recordIndex) {

        rand.setSeed(recordIndex);
        ShoppingCartRecord result;

        switch (getAction()) {
            case NEW:
                result = createShoppingCart();
                activeCarts.add(result);
                result.getItems().add(createCartItem());
                break;

            case COMPLETED:
                int cartIndex = rand.nextInt(activeCarts.size());
                if (!activeCarts.get(cartIndex).getItems().isEmpty()) {
                    result = activeCarts.remove(cartIndex);
                    result.setTransactionCompleted(true);
                    updateTransactionTime(result);
                    break;
                } else {
                    // No items, drop through to update case
                }

            case UPDATE:
                result = activeCarts.get(rand.nextInt(activeCarts.size()));
                // Add item or change quantity
                float updateAction = rand.nextFloat();
                List<CartItem> items = result.getItems();
                if (items.isEmpty() || (updateAction < 0.50)) {
                    items.add(createCartItem());
                } else {
                    // Update quantity. If it's 0, remove it
                    int itemIndex = rand.nextInt(items.size());
                    CartItem item = items.get(itemIndex);
                    item.setQuantity(item.getQuantity() + (rand.nextBoolean() ? 1 : -1));
                    if (item.getQuantity() == 0) {
                        items.remove(itemIndex);
                    }
                }

                updateTransactionTime(result);
                break;

            default:
                throw new RuntimeException("Unknown action");
        }

        return result;
    }

    private void updateTransactionTime(ShoppingCartRecord result) {
        long transTime = result.getTransactionTime();
        transTime += Math.abs(rand.nextGaussian(PER_TRANSACTION_GAP_MS, PER_TRANSACTION_GAP_MS / 5));
        result.setTransactionTime(transTime);

        // Advance curTime so we get new records at a reasonable time.
        curTime = Math.max(curTime, transTime);
    }

    @VisibleForTesting
    public ShoppingCartRecord createShoppingCart() {
        ShoppingCartRecord result = new ShoppingCartRecord();
        result.setTransactionTime(curTime);
        // Advance transaction time.
        curTime += Math.abs(rand.nextGaussian(PER_TRANSACTION_GAP_MS, PER_TRANSACTION_GAP_MS / 5));

        result.setCustomerId("C" + rand.nextInt(100000));
        result.setTransactionId("T" + rand.nextLong());
        result.setTransactionCompleted(false);

        // TODO - 90% are US
        result.setCountry(COUNTRIES[rand.nextInt(COUNTRIES.length)]);
        result.setTransactionCompleted(false);
        // TODO - 95% are empty, otherwise 1 of 10 codes
        result.setCouponCode("");
        // TODO - set payment method, mostly CC, else PayPal
        result.setPaymentMethod("");
        // TODO - base on customer id
        result.setShippingAddress("");
        return result;
    }

    @VisibleForTesting
    public CartItem createCartItem() {
        // Add item
        CartItem newItem = new CartItem();
        newItem.setQuantity(1);
        newItem.setProductId("P" + 0);
        // TODO - set price based on product id
        newItem.setPrice(1.0);
        // Unenriched record doesn't have this data yet
        newItem.setCategory("");
        newItem.setProductName("");
        newItem.setWeightKg(0.0);
        newItem.setUsDollarEquivalent(0.0);

        return newItem;
    }

    // 5% of time it's an add, 95% it's an update, and 5% it's a completed
    private CartAction getAction() {
        if (activeCarts.isEmpty()) {
            return CartAction.NEW;
        }

        float actionVal = rand.nextFloat();
        if ((actionVal < 0.05) && (activeCarts.size() < MAX_ACTIVE_CARTS)) {
            return CartAction.NEW;
        }

        if (actionVal < 0.95) {
            return CartAction.UPDATE;
        }

        return CartAction.COMPLETED;
    }


    private enum CartAction {
        NEW,
        UPDATE,
        COMPLETED
    }

}
