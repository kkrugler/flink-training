package com.ververica.flink.training.solutions.mergesort;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.SortConfig;
import com.ververica.flink.training.provided.ECommerceRecord;
import com.ververica.flink.training.solutions.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Process a partitioned (by report number) set of records. We need to sort them using
 * a merge-sorter, so that we aren't dependent on the amount of available memory.
 *
 */
public class MergeSortRecords extends SortRecordsFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeSortRecords.class);

    private static final int MAX_QUEUED_ELEMENTS = 10_000;
    private static final long MAX_MEMORY = 100 * 1000 * 1000;
    private static final int BUFFER_SIZE = 10 * 1000 * 1000;

    private transient ReportBySorter sorter;
    private transient ArrayBlockingQueue<ReportByRecord> queue;
    private transient AtomicBoolean haveMoreData;
    private transient Thread sortThread;
    private transient AtomicReference<Iterator<ReportByRecord>> sortIterator;
    private transient int upstreamCompleted;

    public MergeSortRecords(List<ReportBy> reports, int numUpstreamOperators) {
        super(reports, numUpstreamOperators);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        SortConfig config = new SortConfig()
                .withMaxMemoryUsage(MAX_MEMORY)
                .withTempFileProvider(() -> makeTempFile("merge-sort").toFile());

        sorter = new ReportBySorter(config, report);
        queue = new ArrayBlockingQueue<>(MAX_QUEUED_ELEMENTS);
        haveMoreData = new AtomicBoolean(true);
        sortIterator = new AtomicReference<>(null);

        upstreamCompleted = 0;

        // We have to run the sorter in the background, so that we're not blocked on it
        // when data arrives.
        sortThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // sorter.sort() will return an iterator when (a) the DataReader has
                    // told it that there's no more data (by returning null), and (b) it
                    // has finished merge-sorting all the data. So we can use that as a
                    // flag to indicate that we're ready to output data.

                    // We'll save it in an Atomic reference, so that this thread can set
                    // the iterator while our process() operator is trying to read it.

                    sortIterator.set(sorter.sort(new DataReader<ReportByRecord>() {
                        @Override
                        public ReportByRecord readNext() throws IOException {
                            // Loop waiting for more data to return for sorting.
                            while (queue.isEmpty()) {
                                if (haveMoreData.get()) {
                                    try {
                                        Thread.sleep(1L);
                                    } catch (InterruptedException e) {
                                        throw new IOException(e);
                                    }
                                } else {
                                    return null;
                                }
                            }

                            return queue.remove();
                        }

                        @Override
                        public int estimateSizeInBytes(ReportByRecord item) {
                            return item.estimateSerializedBytes();
                        }

                        @Override
                        public void close() throws IOException {
                        }
                    }));
                } catch (IOException e) {
                    // TODO - we need to save this exception, and check for this exception
                    // in the processElement method.
                    throw new RuntimeException(e);
                }

            }}, "sorting thread");

        sortThread.start();
    }

    @Override
    public void processElement(Tuple2<Integer, BatchedCarts> in, Context ctx, Collector<ECommerceRecord> out) throws Exception {
        if (in.f1.isEnd()) {

            // We'll get N end records, one for each upstream CreateBatchedCarts. So we have to
            // count the number we've received, and only really finish when we have all N.
            upstreamCompleted++;

            if (upstreamCompleted < numUpstreamOperators) {
                return;
            }

            // No more data coming in, so tell the sorter's DataReader that it can
            // stop waiting when the queue is empty.
            haveMoreData.set(false);

            // Wait for sorter to finish and give us an iterator.
            while (sortIterator.get() == null) {
                // Generate an empty record, which get filtered out in the conversion to string, so
                // that Flink knows we're still alive.
                out.collect(ECommerceRecord.makeEndRecord());
                Thread.sleep(100L);
            }

            // Close (and thus flush) the file where we write the full ECommerceRecord bytes.
            dos.close();
            dos = null;

            RandomAccessFile raf = new RandomAccessFile(tempFile.toFile().getAbsolutePath(), "r");

            Iterator<ReportByRecord> iter = sortIterator.get();
            while (iter.hasNext()) {
                ReportByRecord record = iter.next();
                raf.seek(record.getOffset());
                ECommerceRecord result = new ECommerceRecord();
                result.read(raf);

                out.collect(result);
            }

            raf.close();
        } else {
            byte[] cartData = in.f1.getCartData();
            dos.write(cartData);

            // Write the bytes in the batched record to a temp file,
            // and only put the sortable piece into the queue.
            for (ReportByRecord record : in.f1) {
                record.setOffset(record.getOffset() + outOffset);

                // This will block when the queue becomes full. When that
                // happens, we wind up waiting for the merge-sort thread to
                // fetch an entry to free up some space.
                queue.put(record);
            }

            outOffset += cartData.length;
        }

    }
}
