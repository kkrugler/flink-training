package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.SerializableFunction;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.ECommerceRecord;

public class EndRecordGenerator implements SerializableFunction<Long, ShoppingCartRecord> {

    @Override
    public ShoppingCartRecord apply(Long aLong) {
        ECommerceRecord endRecord = ECommerceRecord.makeEndRecord();
        ShoppingCartRecord result = new ShoppingCartRecord();

        result.setCountry(endRecord.getCountry());
        result.setCouponCode(endRecord.getCouponCode());
        result.setCustomerId(endRecord.getCustomerId());
        result.setPaymentMethod(endRecord.getPaymentMethod());
        result.setShippingCost(endRecord.getShippingCost());

        return result;
    }
}
