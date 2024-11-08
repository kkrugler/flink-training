package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Base class for the in-memory and merge-sort implementations of this ProcessFunction.
 */
public abstract class SortRecordsFunction extends ProcessFunction<Tuple2<Integer, BatchedCarts>, ECommerceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SortRecordsFunction.class);

    private static final int WRITE_BUFFER_SIZE = 10 * 1024 * 1024;

    private final List<ReportBy> reports;
    protected final int numUpstreamOperators;
    protected transient ReportBy report;
    protected transient Path tempFile;
    protected transient DataOutputStream dos;
    protected transient long outOffset;


    public SortRecordsFunction(List<ReportBy> reports, int numUpstreamOperators) {
        this.reports = reports;
        this.numUpstreamOperators = numUpstreamOperators;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // We know that the workflow has partitioned data by report, so that we'll
        // get called with only records for the report index == to our subtask index.
        int reportIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        report = reports.get(reportIndex);

        tempFile = makeTempFile("memory-sort");
        LOGGER.info("Writing eCommerce records to: " + tempFile);

        dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(tempFile.toFile()),
                        WRITE_BUFFER_SIZE));
        outOffset = 0;
    }

    @Override
    public void close() throws Exception {
        if (dos != null) {
            dos.close();
        }

        Files.delete(tempFile);
        tempFile = null;
    }

    public Path makeTempFile(String prefix) throws IOException {
        // TODO - use the TM temp file location as the temp file location.
        return Files.createTempFile(prefix, ".bin");
    }
}
