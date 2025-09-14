/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.CartItemWithShoppingCartInfo;
import com.ververica.flink.training.provided.ShoppingCartWithCost;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * The Review exercise from Ververica's Flink bootcamp training.
 *
 * <p>The goal of this exercise is to filter a data stream of eCommerce shopping cart records to
 * keep only records for completed transactions from the US, and then calculate a total product weight
 * per country, for every minute.
 * </p>
 */
public class BootcampReview3Workflow {

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<ShoppingCartWithCost> resultSink;

    public BootcampReview3Workflow() {}

    public BootcampReview3Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampReview3Workflow setResultSink(Sink<ShoppingCartWithCost> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        cartStream
                .filter(r -> r.isTransactionCompleted())
                .filter(r -> r.getCountry().equals("US"))

                // TODO - use a map function to calculate total cart cost, and return that
                // as a ShoppingCartWithCost record.

                // This map function is there to get the code to compile
                .map(r -> new ShoppingCartWithCost(r, 0.0))
                .sinkTo(resultSink);
    }
}
