package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.util.Collector;

public class CreateTSVRecord extends RichFlatMapFunction<ECommerceRecord, String> {
    @Override
    public void open(OpenContext openContext) throws Exception {
        // TODO - set up TSV writer
    }

    @Override
    public void flatMap(ECommerceRecord value, Collector<String> out) throws Exception {
        if (value.getCountry() == null) {
            return;
        }

        // TODO - convert to TSV
        out.collect(value.toString());
    }
}
