package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.*;
import java.lang.reflect.Type;
import java.sql.Array;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.util.CloseableIterator;

/**
 * A set of carts that share a common key.
 *
 */
public class BatchedCarts implements Iterable<ReportByRecord> {
    private ReportByRecord[] carts;
    private byte[] cartData;

    public BatchedCarts() {}

    public BatchedCarts(ReportByRecord[] carts, byte[] cartData) {
        this.carts = carts;
        this.cartData = cartData;
    }

    public static BatchedCarts makeEndRecord() {
        return new BatchedCarts(new ReportByRecord[0], new byte[0]);
    }

    public boolean isEnd() {
        return (carts.length == 0) && (cartData.length == 0);
    }

    public ReportByRecord[] getCarts() {
        return carts;
    }

    public void setCarts(ReportByRecord[] carts) {
        this.carts = carts;
    }

    public byte[] getCartData() {
        return cartData;
    }

    public void setCartData(byte[] cartData) {
        this.cartData = cartData;
    }

    public int size() {
        return carts.length;
    }

    @Override
    public Iterator<ReportByRecord> iterator() {
        return new Iterator<ReportByRecord>() {

            int curCount = 0;
            int numCarts = carts.length;

            @Override
            public boolean hasNext() {
                return curCount < numCarts;
            }

            @Override
            public ReportByRecord next() {
                if (curCount >= numCarts) {
                    throw new NoSuchElementException();
                }

                return carts[curCount++];
            }

        };
    }

    public static class Builder {

        private ReportBy reportBy;
        private ArrayList<ReportByRecord> carts;
        private ByteArrayOutputStream baos;
        private DataOutputStream dos;

        public Builder(ReportBy reportBy) {
            this.reportBy = reportBy;
            carts = new ArrayList<>();
            baos = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos);
        }

        public int getNumCarts() {
            return carts.size();
        }

        public void add(ECommerceRecord in) throws IOException {
            // Get sortable record for in, put that in array, and write
            // record out to byte array.
            ReportByRecord rbr = reportBy.getSortableRecord(in);
            int offset = baos.size();
            in.write(dos);

            rbr.setOffset(offset);

            carts.add(rbr);
        }

        public BatchedCarts build() {
            Collections.sort(carts);

            return new BatchedCarts(carts.toArray(new ReportByRecord[0]), baos.toByteArray());
        }
    }
}
