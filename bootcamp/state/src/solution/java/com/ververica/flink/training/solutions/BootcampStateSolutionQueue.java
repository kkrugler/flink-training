package com.ververica.flink.training.solutions;

import com.ververica.flink.training.exercises.BootcampStateQueue;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;

import java.io.IOException;
import java.util.*;

/**
 * Provide a state=backed queue for records of type T, which has reasonable
 * performance even with the RocksDB state backend.
 *
 * @param <T>
 */
public class BootcampStateSolutionQueue<T> extends BootcampStateQueue<T> {

    public BootcampStateSolutionQueue(String statePrefix, Class<T> queueClass) {
        super(statePrefix, queueClass);
    }

    private transient ValueState<Long> headState;
    private transient ValueState<Long> tailState;
    private transient MapState<Long, T> queueState;

    public void open(RuntimeContext ctx) {
        headState = ctx.getState(new ValueStateDescriptor<>(statePrefix + "queue-head", Long.class));
        tailState = ctx.getState(new ValueStateDescriptor<>(statePrefix + "queue-tail", Long.class));
        queueState = ctx.getMapState(new MapStateDescriptor<>(statePrefix + "queue", Long.class, queueClass));
    }

    private long getOrZero(ValueState<Long> s) {
        try {
            Long result = s.value();
            return (result == null ? 0 : result);
        } catch (IOException e) {
            throw new RuntimeException("Fetch of queue head/tail state failed", e);
        }
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {

            long curPos = getOrZero(tailState);
            long end = getOrZero(headState);

            @Override
            public boolean hasNext() {
                return curPos < end;
            }

            @Override
            public T next() {
                try {
                    return queueState.get(curPos++);
                } catch (Exception e) {
                    throw new RuntimeException("Fetch of queue entry failed", e);
                }
            }
        };
    }

    @Override
    public int size() {
        return (int)(getOrZero(headState) - getOrZero(tailState));
    }

    @Override
    public boolean offer(T t) {
        try {
            long head = getOrZero(headState);
            queueState.put(head, t);
            headState.update(head + 1);
        } catch (Exception e) {
            throw new RuntimeException("Fetch/update of queue state failed", e);
        }

        return true;
    }

    @Override
    public T poll() {
        long head = getOrZero(headState);
        long tail = getOrZero(tailState);
        if (tail < head) {
            try {
                T result = queueState.get(tail);
                tailState.update(tail + 1);
                return result;
            } catch (Exception e) {
                throw new RuntimeException("Fetch/update of queue state failed", e);
            }
        } else {
            return null;
        }
    }

    @Override
    public T peek() {
        long head = getOrZero(headState);
        long tail = getOrZero(tailState);
        if (tail < head) {
            try {
                return(queueState.get(tail));
            } catch (Exception e) {
                throw new RuntimeException("Fetch/update of queue state failed", e);
            }
        } else {
            return null;
        }
    }
}
