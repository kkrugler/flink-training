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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@TypeInfo(BatchedCartsTypeInfoFactor.class)
public class BatchedCarts {
    private String country;
    private String paymentMethod;

    private byte[] compressedCarts;
    
    List<TrimmedShoppingCart> carts;
    private String key;

    public BatchedCarts() {}

    public BatchedCarts(BatchedCarts base) {
        this.country = base.country;
        this.paymentMethod = base.paymentMethod;
        this.key = base.key;

        this.carts = new ArrayList<>(base.carts.size());
        this.carts.addAll(base.carts);
    }

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

    public List<TrimmedShoppingCart> getCarts() {
        return carts;
    }

    public void setCarts(List<TrimmedShoppingCart> carts) {
        this.carts = carts;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    private static class BatchedCartsTypeInfoFactor extends TypeInfoFactory<BatchedCarts> {

        @Override
        public TypeInformation<BatchedCarts> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            return new BatchedCartsTypeInformation();
        }
    }

    private static class BatchedCartsTypeInformation extends TypeInformation<BatchedCarts> {
        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        public int getTotalFields() {
            return 4;
        }

        @Override
        public Class<BatchedCarts> getTypeClass() {
            return BatchedCarts.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<BatchedCarts> createSerializer(ExecutionConfig config) {
            return new BatchedCartsTypeSerializer(config);
        }

        @Override
        public String toString() {
            return "BatchedCartsTypeInformation{}";
        }

        @Override
        public boolean equals(Object obj) {
            return (obj != null) && (obj instanceof BatchedCartsTypeInformation);
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean canEqual(Object obj) {
            return equals(obj);
        }
    }

    private static class BatchedCartsTypeSerializer extends TypeSerializer<BatchedCarts> {

        private final ExecutionConfig config;

        private transient byte[] serializationBuffer;

        public BatchedCartsTypeSerializer(ExecutionConfig config) {
            this.config = config;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<BatchedCarts> duplicate() {
            return new BatchedCartsTypeSerializer(config);
        }

        @Override
        public BatchedCarts createInstance() {
            return new BatchedCarts();
        }

        @Override
        public BatchedCarts copy(BatchedCarts from) {
            return new BatchedCarts(from);
        }

        @Override
        public BatchedCarts copy(BatchedCarts from, BatchedCarts reuse) {
            // TODO - copy field-by-field into reuse, return that.
            return new BatchedCarts(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(BatchedCarts record, DataOutputView target) throws IOException {
            target.writeUTF(record.country);
            target.writeUTF(record.paymentMethod);
            target.writeInt(record.carts.size());

            if (serializationBuffer == null) {
                serializationBuffer = new byte[MAX_SERIALIZATION_SIZE];


            }

            // TODO - serialize
        }

        @Override
        public BatchedCarts deserialize(DataInputView source) throws IOException {
            return deserialize(new BatchedCarts(), source);
        }

        @Override
        public BatchedCarts deserialize(BatchedCarts reuse, DataInputView source) throws IOException {
            // TODO - deserialize
            return null;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            // TODO - copy
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BatchedCartsTypeSerializer that = (BatchedCartsTypeSerializer) o;

            return Objects.equals(config, that.config);
        }

        @Override
        public int hashCode() {
            return config != null ? config.hashCode() : 0;
        }

        @Override
        public TypeSerializerSnapshot<BatchedCarts> snapshotConfiguration() {
            return new TypeSerializerSnapshot<BatchedCarts>() {
                @Override
                public int getCurrentVersion() {
                    return 0;
                }

                @Override
                public void writeSnapshot(DataOutputView out) throws IOException {
                }

                @Override
                public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
                }

                @Override
                public TypeSerializer<BatchedCarts> restoreSerializer() {
                    return new BatchedCartsTypeSerializer(null);
                }
            };
        }
    }
}
