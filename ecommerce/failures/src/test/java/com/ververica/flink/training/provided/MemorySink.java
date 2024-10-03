package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.MockSink;

import java.util.concurrent.ConcurrentLinkedQueue;

public class MemorySink extends MockSink<String> {

    private static final ConcurrentLinkedQueue<String> QUEUE = new ConcurrentLinkedQueue<>();

    @Override
    public ConcurrentLinkedQueue<String> getSink() {
        return QUEUE;
    }

}
