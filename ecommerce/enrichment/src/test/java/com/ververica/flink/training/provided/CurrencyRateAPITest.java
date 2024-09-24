package com.ververica.flink.training.provided;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class CurrencyRateAPITest {

    @Test
    void testGetRateWithUS() {
        final long seed = 666L;
        CurrencyRateAPI api = new CurrencyRateAPI(seed, 0);

        double rate = api.getRate("US", 0);
        assertEquals(1.0, rate);

        assertEquals(1.0, api.getRate("US", Duration.ofMinutes(1)));
    }

    @Test
    void testGetStableRates() {
        final long seed = 666L;
        final String country = "JP";
        CurrencyRateAPI api = new CurrencyRateAPI(seed, 0);

        // Get rates filled in for 0...5 minutes
        double atFive = api.getRate(country, Duration.ofMinutes(5));

        // Get some target stable values
        double atZero = api.getRate(country, Duration.ofMinutes(0));
        double atThree = api.getRate(country, Duration.ofMinutes(3));

        // Verify they don't change with subsequent calls
        assertEquals(atZero, api.getRate(country, Duration.ofMinutes(0)));
        assertEquals(atThree, api.getRate(country, Duration.ofMinutes(3)));
        assertEquals(atFive, api.getRate(country, Duration.ofMinutes(5)));
    }

    @Test
    void testManyRates() {
        final long seed = 666L;
        final String country = "JP";
        CurrencyRateAPI api = new CurrencyRateAPI(seed, 0);

        api.getRate(country, Duration.ofDays(5));
    }

}