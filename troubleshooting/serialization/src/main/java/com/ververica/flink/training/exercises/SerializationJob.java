/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.training.exercises;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Random;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;

/**
 * Streaming job that tests serialization performance via a simple rebalance
 */
public class SerializationJob {

    /**
     * Creates and starts the streaming job.
     *
     * @throws Exception if the application is misconfigured or fails during job submission
     */
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

        configureEnvironment(env, parameters);


        env.execute(SerializationJob.class.getSimpleName());
    }


    private static void configureEnvironment(StreamExecutionEnvironment env, ParameterTool parameters) {
        final String mode = parameters.get("mode", "pojo");

        switch (mode.toLowerCase()) {
            case "pojo":
                env.getConfig().registerPojoType(DataRecord.class);

                env.addSource(new FakeDataRecordSource())
                        .rebalance()
                        .addSink(new DiscardingSink<>());
                break;

            case "pojoandserializer":
                env.getConfig().registerPojoType(DataRecordCS.class);

                env.addSource(new FakeDataRecordWithSerializerSource())
                        .rebalance()
                        .addSink(new DiscardingSink<>());
                break;

            case "fury":
                env.getConfig().registerPojoType(DataRecordWithFurySerializer.class);

                env.addSource(new FakeDataRecordWithFurySerializerSource())
                        .rebalance()
                        .addSink(new DiscardingSink<>());
                break;

            case "kryo":
                env.getConfig().enableForceKryo();
                env.getConfig().registerKryoType(DataRecord.class);

                env.addSource(new FakeDataRecordSource())
                        .rebalance()
                        .addSink(new DiscardingSink<>());
                break;

                // TODO - add kryoandserializer
            
            case "kryoandfury":
                env.getConfig().enableForceKryo();
                env.getConfig().registerKryoType(DataRecord.class);

                env.getConfig().registerTypeWithKryoSerializer(DataRecord.class,
                        new FuryKryoSerializer<>(DataRecord.class));

                env.addSource(new FakeDataRecordSource())
                        .rebalance()
                        .addSink(new DiscardingSink<>());
                break;

            case "tuple":
                env.addSource(new FakeTupleSource())
                        .rebalance()
                        .addSink(new DiscardingSink<>());
                break;

            case "row":
                env.addSource(new FakeRowSource())
                        .rebalance()
                        .addSink(new DiscardingSink<>());
                break;

            default:
                throw new RuntimeException("Unknown serialization mode: " + mode);
        }
    }

    /*******************************************************************
     * Classes for custom serializer
     * *****************************************************************
     */

    /*
     * DataRecord with Custom Serializer (CS)
     */
    @TypeInfo(DataRecordCSTypeInfoFactory.class)
    public static class DataRecordCS extends DataRecord {

        public DataRecordCS() {
            super();
        }

        public DataRecordCS(DataRecord base) {
            super(base);
        }
    }

    public static class DataRecordCSTypeInfoFactory extends TypeInfoFactory<DataRecordCS> {

        @Override
        public TypeInformation<DataRecordCS> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            return new DataRecordCSTypeInfo();
        }
    }

    /*
     * Type info for a DataRecord with a custom serializer.
     */
    public static class DataRecordCSTypeInfo extends TypeInformation<DataRecordCS> {

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
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 1;
        }

        @Override
        public Class<DataRecordCS> getTypeClass() {
            return DataRecordCS.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<DataRecordCS> createSerializer(ExecutionConfig config) {
            return new DataRecordCSTypeSerializer();
        }

        @Override
        public String toString() {
            return "DataRecordWithTypeInfo";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof DataRecordCSTypeInfo;
        }

        @Override
        public int hashCode() {
            return DataRecordCSTypeInfo.class.hashCode();
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof DataRecordCSTypeInfo;
        }
    }

    public static class DataRecordCSTypeSerializer extends TypeSerializer<DataRecordCS> {

        private transient DataRecordCS buffer;

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<DataRecordCS> duplicate() {
            return this;
        }

        @Override
        public DataRecordCS createInstance() {
            return new DataRecordCS();
        }

        @Override
        public DataRecordCS copy(DataRecordCS from) {
            return copy(from, new DataRecordCS());
        }

        @Override
        public DataRecordCS copy(DataRecordCS from, DataRecordCS reuse) {
            reuse.id = from.id;
            reuse.timestamp = from.timestamp;
            reuse.name = from.name;
            reuse.measurement1 = from.measurement1;
            reuse.measurement2 = from.measurement2;
            reuse.classification = from.classification;
            reuse.details = from.details;
            reuse.validated = from.validated;
            return reuse;
        }

        @Override
        public int getLength() {
            return -1; // Variable data length for record.
        }

        @Override
        public void serialize(DataRecordCS record, DataOutputView target) throws IOException {
            target.writeLong(record.id);
            target.writeLong(record.timestamp);
            target.writeUTF(record.name);
            target.writeDouble(record.measurement1);
            target.writeFloat(record.measurement2);
            target.writeInt(record.classification);
            target.writeUTF(record.details);
            target.writeBoolean(record.validated);
        }

        @Override
        public DataRecordCS deserialize(DataInputView source) throws IOException {
            return deserialize(new DataRecordCS(), source);
        }

        @Override
        public DataRecordCS deserialize(DataRecordCS reuse, DataInputView source) throws IOException {
            reuse.id = source.readLong();
            reuse.timestamp = source.readLong();
            reuse.name = source.readUTF();
            reuse.measurement1 = source.readDouble();
            reuse.measurement2 = source.readFloat();
            reuse.classification = source.readInt();
            reuse.details = source.readUTF();
            reuse.validated = source.readBoolean();
            return reuse;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            if (buffer == null) {
                buffer = new DataRecordCS();
            }

            serialize(deserialize(buffer, source), target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof DataRecordCSTypeSerializer;
        }

        @Override
        public int hashCode() {
            return 0; // TODO - does this matter?
        }

        @Override
        public TypeSerializerSnapshot<DataRecordCS> snapshotConfiguration() {
            // TODO - painful to implement, do we need it for the test?
            return null;
        }
    }

    /*******************************************************************
     * Type/classes for Fury serializer
     * *****************************************************************
     */

    @TypeInfo(DataRecordWithFuryTypeInfoFactory.class)
    public static class DataRecordWithFurySerializer extends DataRecord {

        public DataRecordWithFurySerializer() {
            super();
        }

        public DataRecordWithFurySerializer(DataRecord base) {
            super(base);
        }
    }

    public static class DataRecordWithFuryTypeInfoFactory extends TypeInfoFactory<DataRecordWithFurySerializer> {

        @Override
        public TypeInformation<DataRecordWithFurySerializer> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            return new DataRecordWithFuryTypeInfo();
        }
    }


    public static class DataRecordWithFuryTypeInfo extends TypeInformation<DataRecordWithFurySerializer> {

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
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 1;
        }

        @Override
        public Class<DataRecordWithFurySerializer> getTypeClass() {
            return DataRecordWithFurySerializer.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<DataRecordWithFurySerializer> createSerializer(ExecutionConfig config) {
            return new FurySerializer<>(DataRecordWithFurySerializer.class);
        }

        @Override
        public String toString() {
            return "DataRecordWithFuryTypeInfo";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof DataRecordWithFuryTypeInfo;
        }

        @Override
        public int hashCode() {
            return DataRecordWithFuryTypeInfo.class.hashCode();
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof DataRecordWithFuryTypeInfo;
        }
    }

    /*
     * Fake data sources
     */
    public static abstract class FakeDataSource<T> extends RichParallelSourceFunction<T> {
        private transient volatile boolean running;
        private transient T reuse;
        private transient long id;

        public abstract T createFakeData(DataRecord base);

        public abstract void updateFakeData(T reuse, long id);

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            Random rand = new Random(subtaskIndex);
            DataRecord baseRecord = DataRecord.initFakeRecord(rand, new DataRecord());
            reuse = createFakeData(baseRecord);

            id = subtaskIndex * 1_000_000_000L;
        }

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            running = true;
            while (running) {
                updateFakeData(reuse, id++);
                ctx.collect(reuse);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class FakeDataRecordSource extends FakeDataSource<DataRecord> {

        @Override
        public DataRecord createFakeData(DataRecord reuse) {
            return reuse;
        }

        @Override
        public void updateFakeData(DataRecord reuse, long id) {
            reuse.id = id;
        }
    }


    public static class FakeRowSource extends FakeDataSource<Row> implements ResultTypeQueryable<Row> {

        @Override
        public Row createFakeData(DataRecord base) {
            return Row.of(base.id, base.timestamp, base.name, base.measurement1,
                    base.measurement2, base.classification, base.details, base.validated);
        }

        @Override
        public void updateFakeData(Row reuse, long id) {
            reuse.setField(0, id);
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return DataRecord.getProducedRowType();
        }
    }

    public static class FakeTupleSource extends FakeDataSource<
            Tuple8<Long, Long, String, Double, Float, Integer, String, Boolean>> {


        @Override
        public Tuple8<Long, Long, String, Double, Float, Integer, String, Boolean> createFakeData(DataRecord base) {
            return Tuple8.of(base.id, base.timestamp, base.name, base.measurement1,
                    base.measurement2, base.classification, base.details, base.validated);
        }

        @Override
        public void updateFakeData(Tuple8<Long, Long, String, Double, Float, Integer, String, Boolean> reuse, long id) {
            reuse.f0 = id;
        }
    }

    public static class FakeDataRecordWithSerializerSource extends FakeDataSource<DataRecordCS> {

        @Override
        public DataRecordCS createFakeData(DataRecord base) {
            return new DataRecordCS(base);
        }

        @Override
        public void updateFakeData(DataRecordCS reuse, long id) {
            reuse.id = id;
        }
    }

    public static class FakeDataRecordWithFurySerializerSource extends FakeDataSource<DataRecordWithFurySerializer> {

        @Override
        public DataRecordWithFurySerializer createFakeData(DataRecord base) {
            return new DataRecordWithFurySerializer(base);
        }

        @Override
        public void updateFakeData(DataRecordWithFurySerializer reuse, long id) {
            reuse.id = id;
        }
    }


}
