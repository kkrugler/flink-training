package com.ververica.flink.training.common;

import java.util.Date;

public class CurrencyConversionRecord {
    private String country;
    private Date timestamp;
    private double conversionRate;

    public CurrencyConversionRecord() { }

    // Getters
    public String getCountry() { return country; }
    public Date getTimestamp() { return timestamp; }
    public double getConversionRate() { return conversionRate; }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public void setConversionRate(double conversionRate) {
        this.conversionRate = conversionRate;
    }
}
