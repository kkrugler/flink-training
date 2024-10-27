package com.ververica.flink.training.solutions;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.SortConfig;
import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Process a partitioned (by report number) set of records. We need to sort them using
 * a merge-sorter, so that we aren't dependent on the amount of available memory.
 */
public class MergeSortRecords extends ProcessFunction<Tuple2<Integer, BatchedCarts>, ECommerceRecord> {

    private static final int MAX_QUEUED_ELEMENTS = 10_000;

    private static final long MAX_MEMORY = 100 * 1000 * 1000;

    private final List<ReportBy> reports;

    private transient ECommerceSorter sorter;
    private transient ArrayBlockingQueue<ECommerceRecord> queue;
    private transient AtomicBoolean haveMoreData;
    private transient Thread sortThread;
    private transient AtomicReference<Iterator<ECommerceRecord>> sortIterator;

    public MergeSortRecords(List<ReportBy> reports) {
        this.reports = reports;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // TODO - for each report, do a separate open call with the
        // set of values below (create a new class). We want to divide
        // up the total memory by the number of reports.
        long perReportMemory = MAX_MEMORY / reports.size();

        // TODO - use the TM temp file location as the temp file location
        // for the merge-sort, via the config.
        SortConfig config = new SortConfig()
                .withMaxMemoryUsage(perReportMemory);
        ReportBy reportBy = reports.get(0);
        sorter = new ECommerceSorter(config, reportBy);

        queue = new ArrayBlockingQueue<>(MAX_QUEUED_ELEMENTS);
        haveMoreData = new AtomicBoolean(true);
        sortIterator = new AtomicReference<>(null);

        final RuntimeContext ctx = getRuntimeContext();

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

                    sortIterator.set(sorter.sort(new DataReader<ECommerceRecord>() {
                        @Override
                        public ECommerceRecord readNext() throws IOException {
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
                        public int estimateSizeInBytes(ECommerceRecord item) {
                            // TODO - make this better.
                            return 100;
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
        if (in.f1.getKey() == null) {
            // No more data coming in, so tell the sorter's DataReader that it can
            // stop waiting when the queue is empty.
            haveMoreData.set(false);

            // Wait for sorter to finish and give us an iterator.
            while (sortIterator.get() == null) {
                // Generate an empty record, which get filtered out in the conversion to string, so
                // that Flink knows we're still alive.
                out.collect(new ECommerceRecord());
                Thread.sleep(10L);
            }

            Iterator<ECommerceRecord> iter = sortIterator.get();
            while (iter.hasNext()) {
                // TODO - get the sortable piece from the iterator, then use its
                // offset to get the full record from the disk file. Does that file
                // need to be a random-access file for good performance?
                out.collect(iter.next());
            }
        } else {
            // TODO - write the bytes in the batched record to a temp file,
            // and only put the sortable piece into the queue.
            for (ECommerceRecord record : in.f1) {
                // This will block when the queue becomes full.
                queue.put(record);
            }
        }

    }
}
