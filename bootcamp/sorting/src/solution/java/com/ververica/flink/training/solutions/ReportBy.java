package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.java.functions.KeySelector;

import java.io.Serializable;
import java.util.Comparator;
import java.util.function.Function;

public interface ReportBy extends Comparator<ECommerceRecord>, Serializable {

    // TODO - Remove this - don't have a special key concept for records.
    // TODO - have call to return a merge-sort record, which is comparable
    // and has an offset (to track full record bytes in memory/on disk)
    public String getKey(ECommerceRecord in);
}
