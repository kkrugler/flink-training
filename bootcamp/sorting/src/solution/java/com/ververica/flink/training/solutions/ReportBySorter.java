package com.ververica.flink.training.solutions;

import com.fasterxml.sort.*;

import java.io.*;

public class ReportBySorter extends Sorter<ReportByRecord>
{
    public ReportBySorter(ReportBy reportBy) {
        this(new SortConfig(), reportBy);
    }

    public ReportBySorter(SortConfig config, ReportBy reportBy) {
        super(config,
                new DataReaderFactory<ReportByRecord>() {
                    @Override
                    public DataReader<ReportByRecord> constructReader(InputStream is) throws IOException {
                        return new ReportByRecordDataReader(reportBy, is);
                    }
                },

                new DataWriterFactory<ReportByRecord>() {
                    @Override
                    public DataWriter<ReportByRecord> constructWriter(OutputStream os) throws IOException {
                        return new ReportByRecordDataWriter(os);
                    }
                },

                reportBy.getSortableRecord(null));
    }

    private static class ReportByRecordDataReader extends DataReader<ReportByRecord> {

        private ReportBy reportBy;
        private DataInputStream dis;

        public ReportByRecordDataReader(ReportBy reportBy, InputStream in) {
            this.reportBy = reportBy;
            this.dis = new DataInputStream(in);
        }

        @Override
        public ReportByRecord readNext() throws IOException {
            if (dis.available() == 0) {
                return null;
            }

            ReportByRecord result = reportBy.getSortableRecord(null);
            result.read(dis);
            return result;
        }

        @Override
        public int estimateSizeInBytes(ReportByRecord item) {
            return item.estimateSerializedBytes();
        }

        @Override
        public void close() throws IOException {
            if (dis != null) {
                dis.close();
                dis = null;
            }
        }
    }

    private static class ReportByRecordDataWriter extends DataWriter<ReportByRecord> {

        private DataOutputStream dos;

        public ReportByRecordDataWriter(OutputStream out) {
            this.dos = new DataOutputStream(out);
        }

        @Override
        public void writeEntry(ReportByRecord item) throws IOException {
            item.write(dos);
        }

        @Override
        public void close() throws IOException {
            if (dos != null) {
                dos.close();;
                dos = null;
            }
        }
    }


}
