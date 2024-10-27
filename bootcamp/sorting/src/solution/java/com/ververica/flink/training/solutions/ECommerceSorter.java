package com.ververica.flink.training.solutions;

import com.fasterxml.sort.*;
import com.fasterxml.sort.std.ByteArrayComparator;
import com.fasterxml.sort.std.RawTextLineReader;
import com.fasterxml.sort.std.RawTextLineWriter;
import com.ververica.flink.training.provided.ECommerceRecord;

import javax.xml.crypto.Data;
import java.io.*;

public class ECommerceSorter extends Sorter<ECommerceRecord>
{
    public final static long MAX_HEAP_FOR_PRESORT = 256L * 1024 * 1024;

    public ECommerceSorter(ReportBy reportBy) {
        this(new SortConfig(), reportBy);
    }

    public ECommerceSorter(SortConfig config, ReportBy reportBy) {
        super(config,
                new DataReaderFactory<ECommerceRecord>() {
                    @Override
                    public DataReader<ECommerceRecord> constructReader(InputStream is) throws IOException {
                        return new ECommerceDataReader(is);
                    }
                },

                new DataWriterFactory<ECommerceRecord>() {
                    @Override
                    public DataWriter<ECommerceRecord> constructWriter(OutputStream os) throws IOException {
                        return new ECommerceDataWriter(os);
                    }
                },

                reportBy);
    }

    private static class ECommerceDataReader extends DataReader<ECommerceRecord> {

        private DataInputStream dis;

        public ECommerceDataReader(InputStream in) {
            this.dis = new DataInputStream(in);
        }

        @Override
        public ECommerceRecord readNext() throws IOException {
            if (dis.available() == 0) {
                return null;
            }

            ECommerceRecord result = new ECommerceRecord();
            result.read(dis);
            return result;
        }

        @Override
        public int estimateSizeInBytes(ECommerceRecord item) {
            // TODO - set better estimate
            return 100;
        }

        @Override
        public void close() throws IOException {
            if (dis != null) {
                dis.close();
                dis = null;
            }
        }
    }

    private static class ECommerceDataWriter extends DataWriter<ECommerceRecord> {

        private DataOutputStream dos;

        public ECommerceDataWriter(OutputStream out) {
            this.dos = new DataOutputStream(out);
        }

        @Override
        public void writeEntry(ECommerceRecord item) throws IOException {
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
