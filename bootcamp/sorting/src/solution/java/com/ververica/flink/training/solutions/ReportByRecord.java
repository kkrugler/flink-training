package com.ververica.flink.training.solutions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;

public abstract class ReportByRecord implements Comparable<ReportByRecord>, Comparator<ReportByRecord> {

    private long offset;

    public ReportByRecord() {}

    public ReportByRecord(long offset) {

        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void read(DataInputStream in) throws IOException {
        offset = in.readLong();
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeLong(offset);
    }

    public abstract int estimateSerializedBytes();

    public static int getSerializedSize() {
        return Long.BYTES;
    }
}
