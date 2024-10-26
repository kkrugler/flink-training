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
    private int numCarts;
    private byte[] compressedCarts;

    public BatchedCarts() {}

    public BatchedCarts(String key, int numCarts, byte[] compressedCarts) {
        this.key = key;
        this.numCarts = numCarts;
        this.compressedCarts = compressedCarts;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getNumCarts() {
        return numCarts;
    }

    public void setNumCarts(int numCarts) {
        this.numCarts = numCarts;
    }

    public byte[] getCompressedCarts() {
        return compressedCarts;
    }

    public void setCompressedCarts(byte[] compressedCarts) {
        this.compressedCarts = compressedCarts;
    }

    @Override
    public Iterator<ECommerceRecord> iterator() {
        return new CloseableIterator<ECommerceRecord>() {

            int curCount = 0;
            DataInputStream dis = new DataInputStream(makeGZIPInputStream(compressedCarts));

            @Override
            public boolean hasNext() {
                return curCount < numCarts;
            }

            @Override
            public ECommerceRecord next() {
                if (curCount >= numCarts) {
                    throw new NoSuchElementException();
                }

                curCount++;

                try {
                    ECommerceRecord result = new ECommerceRecord();
                    result.read(dis);
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException("Corrupt data", e);
                }

            }

            @Override
            public void close() throws Exception {
                dis.close();
            }

        };
    }

    private static InputStream makeGZIPInputStream(byte[] data) {
        try {
            return new GZIPInputStream(new ByteArrayInputStream(data));
        } catch (IOException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }

    public static class Builder {

        private String key;
        private int numCarts;
        private DataOutputStream compressedDOS;
        private ByteArrayOutputStream compressedBytes;

        public Builder() {
            compressedBytes = new ByteArrayOutputStream();

            try {
                compressedDOS = new DataOutputStream(new GZIPOutputStream(compressedBytes, true));
            } catch (IOException e) {
                throw new RuntimeException("Impossible exception", e);
            }

            numCarts = 0;
        }

        public int getNumCarts() {
            return numCarts;
        }

        public void add(ECommerceRecord in) throws IOException {
            // TODO - Use KeySelector
            String addedKey = in.getCountry();
            if (numCarts == 0) {
                key = addedKey;
            } else if (!key.equals(addedKey)) {
                throw new IllegalArgumentException("Key doesn't match batch!");
            }

            in.write(compressedDOS);

            numCarts++;
        }

        // TODO - would it help to sort each batch before building?
        public BatchedCarts build() {
            try {
                compressedDOS.close();
            } catch (IOException e) {
                throw new RuntimeException("Error flushing output streams", e);
            }

            return new BatchedCarts(key, numCarts, compressedBytes.toByteArray());
        }
    }
}
