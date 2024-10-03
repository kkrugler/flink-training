package com.ververica.flink.training.provided;

import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TransactionalMemorySink implements StatefulSink<String, List<String>> {

    // This is a static class member, because it represents the persisted transactional
    // data (e.g. a DB's committed set of records).
    private static final ConcurrentLinkedQueue<String> QUEUE = new ConcurrentLinkedQueue<>();

    // This is a static class member, because it represents the sink persistent global state
    // (e.g. a DB's set of transactions), so we don't want it cleared when the workflow is
    // re-started after a failure.
    private static final ConcurrentHashMap<Long, List<String>> TRANSACTIONS = new ConcurrentHashMap<>();

    @Override
    public StatefulSinkWriter<String, List<String>> createWriter(InitContext context) throws IOException {
        return new MemorySinkWriter(context.getSubtaskId());
    }

    @Override
    public SimpleVersionedSerializer<List<String>> getWriterStateSerializer() {
        return new StringListSerializer();
    }

    private class MemorySinkWriter implements StatefulSinkWriter<String, List<String>> {

        private final int subtaskId;
        private long transactionId = 0;
        private List<String> currentTransaction = new ArrayList<>();

        public MemorySinkWriter(int subtaskId) {
            this.subtaskId = subtaskId;
        }

        @Override
        public void write(String element, Context context) throws IOException, InterruptedException {
            currentTransaction.add(element);
        }

        @Override
        public List<List<String>> snapshotState(long checkpointId) throws IOException {
            List<List<String>> state = new ArrayList<>(TRANSACTIONS.values());
            state.add(new ArrayList<>(currentTransaction));
            return state;
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            if (endOfInput || !currentTransaction.isEmpty()) {
                TRANSACTIONS.put(transactionId++, new ArrayList<>(currentTransaction));
                QUEUE.addAll(currentTransaction);
                currentTransaction.clear();
            }
        }

        @Override
        public void close() throws Exception {
            // No-op for in-memory sink
        }
    }

    private static class StringListSerializer implements SimpleVersionedSerializer<List<String>> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(List<String> state) throws IOException {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(state);
                return bos.toByteArray();
            }
        }

        @Override
        public List<String> deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                 ObjectInputStream ois = new ObjectInputStream(bis)) {
                return (List<String>) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize state", e);
            }
        }
    }

    public ConcurrentLinkedQueue<String> getSink() {
        return QUEUE;
    }

    public TransactionalMemorySink reset() {
        QUEUE.clear();
        return this;
    }
}