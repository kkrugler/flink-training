package com.ververica.flink.training.common;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

/**
 * A simple wrapper that can be used, with an appropriately serializable record provider, to add as a source via the new
 * Flink source API. This source doesn't support checkpointing.
 *
 */
@SuppressWarnings("serial")
public class FakeParallelSource<T>
        implements Source<T, FakeParallelSource.NoOpSourceSplit, FakeParallelSource.NoOpEnumState> {

    private static final long DEFAULT_DELAY = 0;
    private static final boolean DEFAULT_BOUNDED = false;

    private final int parallelism;
    private final long numRecords;
    private final SerializableFunction<Long, T> recordProvider;
    private final long delay;
    private final boolean bounded;

    private transient long curRecordIndex;
    private transient long nextRecordIndex;
    private transient long recordIndexDelta;
    private transient long numRemainingRecords;

    public FakeParallelSource(int parallelism, long numRecords, SerializableFunction<Long, T> recordProvider) {
        this(parallelism, numRecords, DEFAULT_DELAY, DEFAULT_BOUNDED, recordProvider);
    }

    public FakeParallelSource(int parallelism, long numRecords, long delay, boolean bounded, SerializableFunction<Long, T> recordProvider) {
        Preconditions.checkArgument(parallelism > 0);
        Preconditions.checkArgument(numRecords >= 0);
        Preconditions.checkArgument(delay >= 0);
        Preconditions.checkNotNull(recordProvider);

        this.parallelism = parallelism;
        this.numRecords = numRecords;
        this.recordProvider = recordProvider;
        this.delay = delay;
        this.bounded = bounded;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, NoOpSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {

        // If we're unbounded, and unending, then just propagate the max count
        int subtaskIndex = readerContext.getIndexOfSubtask();
        if (!bounded && (numRecords == Long.MAX_VALUE)) {
            numRemainingRecords = numRecords;
        } else {
            long numRecordsRemaining = numRecords;
            int numSubtasksRemaining = parallelism;

            for (int i = 0; i <= subtaskIndex; i++) {
                if (numSubtasksRemaining == 0) {
                    throw new RuntimeException("WTF");
                }
                long subtaskRecords = numRecordsRemaining / numSubtasksRemaining;

                if (i == subtaskIndex) {
                    numRemainingRecords = subtaskRecords;
                } else {
                    numRecordsRemaining -= subtaskRecords;
                    numSubtasksRemaining -= 1;
                }
            }
        }

        curRecordIndex = 0;
        nextRecordIndex = subtaskIndex;
        recordIndexDelta = parallelism;

        return new SourceReader<>() {

            @Override
            public void close() throws Exception {
            }

            @Override
            public void start() {
            }

            @Override
            public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
                if (numRemainingRecords > 0) {
                    T nextRecord = recordProvider.apply(curRecordIndex);
                    if (nextRecord == null) {
                        return InputStatus.NOTHING_AVAILABLE;
                    }

                    if (curRecordIndex == nextRecordIndex) {
                        if (delay > 0) {
                            Thread.sleep(delay);
                        }

                        output.collect(nextRecord);

                        numRemainingRecords--;
                        nextRecordIndex += recordIndexDelta;
                    }

                    curRecordIndex++;
                    return InputStatus.MORE_AVAILABLE;
                } else if (bounded) {
                    return InputStatus.END_OF_INPUT;
                } else {
                    // Since we're unbounded.
                    return InputStatus.NOTHING_AVAILABLE;
                }
            }

            @Override
            public List<NoOpSourceSplit> snapshotState(long checkpointId) {
                return null;
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void addSplits(List<NoOpSourceSplit> splits) {
            }

            @Override
            public void notifyNoMoreSplits() {
            }
        };
    }

    @Override
    public SplitEnumerator<NoOpSourceSplit, NoOpEnumState> createEnumerator(
            SplitEnumeratorContext<NoOpSourceSplit> enumContext) throws Exception {
        return new SplitEnumerator<>() {

            @Override
            public void start() {
            }

            @Override
            public void handleSplitRequest(int subtaskId, String requesterHostname) {
            }

            @Override
            public void addSplitsBack(List<NoOpSourceSplit> splits, int subtaskId) {
            }

            @Override
            public void addReader(int subtaskId) {
            }

            @Override
            public NoOpEnumState snapshotState(long checkpointId) throws Exception {
                return new NoOpEnumState();
            }

            @Override
            public void close() throws IOException {
            }

        };
    }

    @Override
    public SplitEnumerator<NoOpSourceSplit, NoOpEnumState> restoreEnumerator(
            SplitEnumeratorContext<NoOpSourceSplit> enumContext, NoOpEnumState checkpoint)
            throws Exception {
        return new SplitEnumerator<>() {

            @Override
            public void start() {
            }

            @Override
            public void handleSplitRequest(int subtaskId, String requesterHostname) {
            }

            @Override
            public void addSplitsBack(List<NoOpSourceSplit> splits, int subtaskId) {
            }

            @Override
            public void addReader(int subtaskId) {
            }

            @Override
            public NoOpEnumState snapshotState(long checkpointId) throws Exception {
                return new NoOpEnumState();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    /** Mock enumerator state. */
    public static class NoOpEnumState {
    }

    @Override
    public SimpleVersionedSerializer<NoOpSourceSplit> getSplitSerializer() {
        return new SimpleVersionedSerializer<>() {

            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(NoOpSourceSplit obj) throws IOException {
                return new byte[0];
            }

            @Override
            public NoOpSourceSplit deserialize(int version, byte[] serialized) throws IOException {
                return new NoOpSourceSplit();
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<NoOpEnumState> getEnumeratorCheckpointSerializer() {
        return new NoOpEnumStateSerializer();
    }

    /**
     * Our FakeParallelSource doesn't support splitting, so this is a no-op implementation.
     *
     */
    public static class NoOpSourceSplit implements SourceSplit {

        @Override
        public String splitId() {
            return "split-0";
        }
    }

    public static class NoOpEnumStateSerializer
            implements SimpleVersionedSerializer<NoOpEnumState> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(NoOpEnumState obj) throws IOException {
            return new byte[0];
        }

        @Override
        public NoOpEnumState deserialize(int version, byte[] serialized) throws IOException {
            return new NoOpEnumState();
        }
    }

}
