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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import com.ververica.flink.training.solutions.ECommerceRecord;

public class BatchedCarts implements Iterable<ECommerceRecord> {
    private String country;
    private String paymentMethod;
    private byte[] compressedCarts;

    public BatchedCarts() {}

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public byte[] getCompressedCarts() {
        return compressedCarts;
    }

    public void setCompressedCarts(byte[] compressedCarts) {
        this.compressedCarts = compressedCarts;
    }

    @Override
    public Iterator<ECommerceRecord> iterator() {
        return new Iterator<ECommerceRecord>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public ECommerceRecord next() {
                return null;
            }
        };
    }

    public static class Builder {

        private int numCarts = 0;

        public Builder() {}

        public int getNumCarts() {
            return numCarts;
        }

        public void add(ECommerceRecord in) {
            numCarts++;
        }

        public BatchedCarts build() {
            return null;
        }
    }
}
