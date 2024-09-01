package com.ververica.flink.training.solutions;

import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BetterShoppingCartRecordTest {

    @Test
    public void testSerialization() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(BetterShoppingCartRecord.class);
    }
}