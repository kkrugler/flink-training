package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.FakeParallelSource;
import com.ververica.flink.training.common.SerializableFunction;

import java.time.Duration;

public class ECommerceEndSource extends FakeParallelSource<ECommerceRecord> {
    public ECommerceEndSource(long parallelism) {
        super(parallelism, getECommerceEndGenerator());
    }

    public ECommerceEndSource(long parallelism, long delay, boolean bounded) {
        super(parallelism, delay, bounded, getECommerceEndGenerator());
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
