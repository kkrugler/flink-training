package com.ververica.flink.training.exercises;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.fury.Fury;
import io.fury.config.Language;
import io.fury.exception.FuryException;
import io.fury.memory.MemoryBuffer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.api.java.typeutils.runtime.NoFetchingInput;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.EOFException;
import java.io.IOException;

public class FurySerializer<T> extends TypeSerializer<T> {

    private final Class<T> clazz;

    private transient Fury fury;

    private transient DataOutputView previousOut;
    private transient DataInputView previousIn;

    private transient Input input;
    private transient Output output;
    private transient T tempRecord;
    private transient MemoryBuffer buffer;

    public FurySerializer(Class<T> clazz) {
        super();

        this.clazz = clazz;
    }

    private Fury getFury() {
        if (fury == null) {
            fury = Fury.builder()
                    .withLanguage(Language.JAVA)
                    .requireClassRegistration(true)
                    .build();

            fury.register(clazz);
        }

        return fury;
    }

    private T getTempRecord() {
        if (tempRecord == null) {
            tempRecord = createInstance();
        }

        return tempRecord;
    }

    private MemoryBuffer getBuffer() {
        if (buffer == null) {
            buffer = MemoryBuffer.newHeapBuffer(1000);
        }

        return buffer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return this;
    }

    @Override
    public T createInstance() {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Class can't be created using empty constructor", e);
        }
    }

    @Override
    public T copy(T from) {
        return copy(from, null);
    }

    @Override
    public T copy(T from, T reuse) {
        getFury().serializeJavaObject(getBuffer(), from);
        return getFury().deserializeJavaObject(getBuffer(), clazz);
    }

    @Override
    public int getLength() {
        return -1; // Variable data length for record.
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (target != previousOut) {
            DataOutputViewStream outputStream = new DataOutputViewStream(target);
            output = new Output(outputStream);
            previousOut = target;
        }

        try {
            getFury().serializeJavaObject(output, record);
            output.flush();
        } catch (FuryException fe) {
            // make sure that the Fury output buffer is cleared in case that we can recover from
            // the exception (e.g. EOFException which denotes buffer full)
            output.clear();

            Throwable cause = fe.getCause();
            if (cause instanceof EOFException) {
                throw (EOFException) cause;
            } else {
                throw fe;
            }
        }

    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        return deserialize(createInstance(), source);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        if (source != previousIn) {
            DataInputViewStream inputStream = new DataInputViewStream(source);
            input = new NoFetchingInput(inputStream);
            previousIn = source;
        }

        try {
            return getFury().deserializeJavaObject(input, clazz);
        } catch (FuryException fe) {
            Throwable cause = fe.getCause();

            if (cause instanceof EOFException) {
                throw (EOFException) cause;
            } else {
                throw fe;
            }
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(getTempRecord(), source), target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FurySerializer;
    }

    @Override
    public int hashCode() {
        return 0; // TODO - does this matter?
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        // TODO - painful to implement, do we need it for the test?
        return null;
    }
}
