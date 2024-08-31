package com.ververica.flink.training.common;

import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class ShoppingCartRecordTest {

    /**
     * This test is disabled since it fails without some modifications to the
     * shopping cart record, due to SimpleList using generics.
     */
    @Test
    @Disabled
    public void testSerializable() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(ShoppingCartRecord.class);
    }

}