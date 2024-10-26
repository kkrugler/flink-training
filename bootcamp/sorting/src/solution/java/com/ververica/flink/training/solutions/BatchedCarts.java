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
 * A set of carts that share a common key, where we compress the
 * carts using Gzip to reduce record size and thus
 */
public class BatchedCarts implements Iterable<ECommerceRecord> {
    private String key;
    private ECommerceRecord[] carts;

    public BatchedCarts() {}

    public BatchedCarts(String key, ECommerceRecord[] carts) {
        this.key = key;
        this.carts = carts;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public ECommerceRecord[] getCarts() {
        return carts;
    }

    public void setCarts(ECommerceRecord[] carts) {
        this.carts = carts;
    }

    public int size() {
        return carts.length;
    }

    @Override
    public Iterator<ECommerceRecord> iterator() {
        return new Iterator<ECommerceRecord>() {

            int curCount = 0;
            int numCarts = carts.length;

            @Override
            public boolean hasNext() {
                return curCount < numCarts;
            }

            @Override
            public ECommerceRecord next() {
                if (curCount >= numCarts) {
                    throw new NoSuchElementException();
                }

                return carts[curCount++];
            }

        };
    }

    public static class Builder {

        private String key;
        private ReportBy reportBy;
        private ArrayList<ECommerceRecord> carts;

        public Builder(ReportBy reportBy) {
            this.reportBy = reportBy;
            carts = new ArrayList<>();
        }

        public int getNumCarts() {
            return carts.size();
        }

        public void add(ECommerceRecord in) throws IOException {

            // TODO - Use KeySelector
            String newKey = reportBy.getKey(in);
            if (carts.isEmpty()) {
                key = newKey;
            } else if (!key.equals(newKey)) {
                throw new IllegalArgumentException("Key doesn't match batch!");
            }

            carts.add(in);
        }

        public BatchedCarts build() {
            Collections.sort(carts, reportBy);

            return new BatchedCarts(key, carts.toArray(new ECommerceRecord[0]));
        }
    }
}
