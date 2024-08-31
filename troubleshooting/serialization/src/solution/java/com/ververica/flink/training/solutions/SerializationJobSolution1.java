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

package com.ververica.flink.training.solutions;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;

/** Streaming job that tests serialization performance via a simple rebalance */
public class SerializationJobSolution1 {

    /**
     * Creates and starts the streaming job.
     *
     * @throws Exception if the application is misconfigured or fails during job submission
     */
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

        env.addSource(new FakeDataRecordSource())
                .name("FakeRecordSource")
                .uid("FakeRecordSource")
                .rebalance()
                .addSink(new DiscardingSink<>());

        env.execute(SerializationJobSolution1.class.getSimpleName());
    }

    public static class DataRecord2 extends DataRecord {
        public DataRecord2() {
            super();
        }
    }

    public static class DataRecord {
        public long id;
        public long timestamp;
        public String name;
        public double measurement1;
        public float measurement2;
        public int classification;
        public String details;
        public boolean validated;

        public DataRecord() {}

        public static DataRecord2 makeFakeRecord(Random rand) {
            DataRecord2 result = new DataRecord2();
            result.id = rand.nextLong();
            result.timestamp = rand.nextLong();
            result.name = "RecordName-" + rand.nextInt(10000);
            result.measurement1 = rand.nextDouble();
            result.measurement2 = rand.nextFloat();
            result.classification = rand.nextInt();
            result.details = makeRandomString(rand);
            result.validated = rand.nextBoolean();
            return result;
        }

        private static final String[] RANDOM_WORDS = new String[] {
                "some",
                "any",
                "with",
                "because",
                "maybe",
                "definitely",
                "that",
                "those"

        };

        private static String makeRandomString(Random rand) {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    result.append(' ');
                }

                result.append(RANDOM_WORDS[rand.nextInt(RANDOM_WORDS.length)]);
            }

            return result.toString();
        }
    }

    public static class FakeDataRecordSource extends RichParallelSourceFunction<DataRecord2> {

        private transient Random rand;
        private transient volatile boolean running;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            rand = new Random(getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void run(SourceContext<DataRecord2> ctx) throws Exception {
            running = true;
            while (running) {
                ctx.collect(DataRecord.makeFakeRecord(rand));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
