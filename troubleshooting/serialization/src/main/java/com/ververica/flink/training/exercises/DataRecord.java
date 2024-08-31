package com.ververica.flink.training.exercises;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * Simple record with a variety of field types.
 */

public class DataRecord {
    public long id;
    public long timestamp;
    public String name;
    public double measurement1;
    public float measurement2;
    public int classification;
    public String details;
    public boolean validated;

    public DataRecord() {
    }

    public DataRecord(DataRecord base) {
        this.id = base.id;
        this.timestamp = base.timestamp;
        this.name = base.name;
        this.measurement1 = base.measurement1;
        this.measurement2 = base.measurement2;
        this.classification = base.classification;
        this.details = base.details;
        this.validated = base.validated;
    }

    public static DataRecord initFakeRecord(Random rand, DataRecord result) {
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

    private static final int NUM_RANDOM_WORDS = 4;

    private static final String[] RANDOM_WORDS = new String[]{
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
        for (int i = 0; i < NUM_RANDOM_WORDS; i++) {
            if (i > 0) {
                result.append(' ');
            }

            result.append(RANDOM_WORDS[rand.nextInt(RANDOM_WORDS.length)]);
        }

        return result.toString();
    }

    public static TypeInformation<Row> getProducedRowType() {

        return Types.ROW(
                Types.LONG,
                Types.LONG,
                Types.STRING,
                Types.DOUBLE,
                Types.FLOAT,
                Types.INT,
                Types.STRING,
                Types.BOOLEAN);
    }

}
