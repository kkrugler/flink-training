package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.FakeParallelSource;
import com.ververica.flink.training.common.SerializableFunction;

import java.time.Duration;

public class ECommerceEndSource extends FakeParallelSource<ECommerceRecord> {
    public ECommerceEndSource(long numRecords) {
        super(numRecords, 0L, true, getECommerceEndGenerator());
    }

    public ECommerceEndSource(long numRecords, long delay, boolean bounded) {
        super(numRecords, delay, bounded, getECommerceEndGenerator());
    }

    private static SerializableFunction<Long, ECommerceRecord> getECommerceEndGenerator() {
        return new SerializableFunction<Long, ECommerceRecord>() {
            @Override
            public ECommerceRecord apply(Long recordIndex) {
                return ECommerceRecord.makeEndRecord();
            }
        };
    }

}
