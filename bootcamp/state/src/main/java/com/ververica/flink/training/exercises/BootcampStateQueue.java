/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.exercises;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provide a state=backed queue for records of type T, which has reasonable performance even with
 * the RocksDB state backend.
 *
 * @param <T>
 */
public class BootcampStateQueue<T> extends AbstractQueue<T> {

    protected String statePrefix;
    protected Class<T> queueClass;

    /**
     * Create new state that supports a FIFO queue.
     *
     * @param statePrefix String to use as prefix for all state names, to avoid collisions.
     * @param queueClass Class for elements in the queue.
     */
    public BootcampStateQueue(String statePrefix, Class<T> queueClass) {
        this.statePrefix = statePrefix;
        this.queueClass = queueClass;
    }

    /**
     * Initialize the state.
     *
     * @param ctx Context passed to Flink funtion open() method.
     */
    public void open(RuntimeContext ctx) {
        // TODO - define state we need.
    }

    @Override
    public Iterator<T> iterator() {
        // TODO - iterator over entries in queue
        return new Iterator<T>() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                throw new NoSuchElementException();
            }
        };
    }

    @Override
    public int size() {
        // TODO - return elements in queue state

        return 0;
    }

    @Override
    public boolean offer(T t) {
        // TODO - add entry to queue
        return false;
    }

    @Override
    public T poll() {
        // TODO remove and return tail element from queue, or null
        return null;
    }

    @Override
    public T peek() {
        // TODO - return tail element from queue without removing
        // it, or return null if empty.
        return null;
    }
}
