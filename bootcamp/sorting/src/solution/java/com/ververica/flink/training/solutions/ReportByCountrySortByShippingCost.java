package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;

public class ReportByCountrySortByShippingCost implements ReportBy {

    @Override
    public String getKey(ECommerceRecord value) {
        return value.getCountry();
    }

    @Override
    public int compare(ECommerceRecord o1, ECommerceRecord o2) {
        int result = o1.getCountry().compareTo(o2.getCountry());
        if (result == 0) {
            result = Double.compare(o1.getShippingCost(), o2.getShippingCost());
        }

        return result;
    }
}
