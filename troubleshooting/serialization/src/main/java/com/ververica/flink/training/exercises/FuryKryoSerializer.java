package com.ververica.flink.training.exercises;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.fury.Fury;
import io.fury.config.Language;

import java.io.Serializable;

/*
 * Kryo custom serializer that uses Fury.
 */
public class FuryKryoSerializer<T> extends Serializer<T> implements Serializable {

    private Class<T> clazz;

    private transient Fury fury;

    /*
     * We require that the target class be passed to the constructor so that
     * we can safely use the more efficient mode of requiring class registration.
     */
    public FuryKryoSerializer(Class<T> clazz) {
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

    @Override
    public void write(Kryo kryo, Output output, T object) {
        getFury().serializeJavaObject(output, object);
    }

    @Override
    public T read(Kryo kryo, Input input, Class<T> type) {
        return getFury().deserializeJavaObject(input, type);
    }
}
