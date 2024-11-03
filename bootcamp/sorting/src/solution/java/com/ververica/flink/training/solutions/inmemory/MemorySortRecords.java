package com.ververica.flink.training.solutions.inmemory;

import com.ververica.flink.training.provided.ECommerceRecord;
import com.ververica.flink.training.solutions.BatchedCarts;
import com.ververica.flink.training.solutions.ReportBy;
import com.ververica.flink.training.solutions.ReportByRecord;
import com.ververica.flink.training.solutions.SortRecordsFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Process a partitioned (by report number) set of records. We need to sort them using
 * a merge-sorter, so that we aren't dependent on the amount of available memory.
 */
public class MemorySortRecords extends SortRecordsFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemorySortRecords.class);

    private static final int WRITE_BUFFER_SIZE = 10 * 1024 * 1024;
    private static final int READ_BUFFER_SIZE = 256;

    private final List<ReportBy> reports;
    private final int numUpstreamOperators;

    private transient Path tempFile;
    private transient DataOutputStream dos;
    private transient long outOffset;
    private transient int upstreamCompleted;
    private List<ReportByRecord> records;

    public MemorySortRecords(List<ReportBy> reports, int numUpstreamOperators) {
        this.reports = reports;
        this.numUpstreamOperators = numUpstreamOperators;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // TODO - for each report, do a separate open call with the
        // set of values below (create a new class). We want to divide
        // up the total memory by the number of reports.

        tempFile = Files.createTempFile("merge-sort", ".bin");
        LOGGER.info("Writing records to: " + tempFile);

        dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(tempFile.toFile()),
                        WRITE_BUFFER_SIZE));
        outOffset = 0;
        upstreamCompleted = 0;

        records = new ArrayList<>();
    }

    @Override
    public void close() throws Exception {
        if (dos != null) {
            dos.close();
        }

        Files.delete(tempFile);
        tempFile = null;
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

            // Close (and thus flush) the file where we write the full ECommerceRecord bytes.
            dos.close();
            dos = null;

            RandomAccessFile raf = new RandomAccessFile(tempFile.toFile().getAbsolutePath(), "r");

            Collections.sort(records, reports.get(0).getSortableRecord(null));

            ECommerceRecord result = new ECommerceRecord();
            for (ReportByRecord record : records) {
                raf.seek(record.getOffset());
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
                records.add(record);
            }

            outOffset += cartData.length;
        }

    }
}
