package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;
import org.apache.flink.api.java.functions.KeySelector;

import java.io.Serializable;
import java.util.Comparator;
import java.util.function.Function;

public interface ReportBy extends Comparator<ECommerceRecord>, Serializable {

    public ReportByRecord getSortableRecord(ECommerceRecord in);

}
