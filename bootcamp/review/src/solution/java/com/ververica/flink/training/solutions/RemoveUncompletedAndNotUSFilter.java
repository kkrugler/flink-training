package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.functions.FilterFunction;

import com.ververica.flink.training.common.ShoppingCartRecord;

public class RemoveUncompletedAndNotUSFilter implements FilterFunction<ShoppingCartRecord> {
    @Override
    public boolean filter(ShoppingCartRecord in) throws Exception {
        return in.isTransactionCompleted() && (in.getCountry().equals("US"));
    }
}
