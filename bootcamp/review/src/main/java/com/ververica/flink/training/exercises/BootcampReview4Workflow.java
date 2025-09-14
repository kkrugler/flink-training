/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.CartItemWithShoppingCartInfo;
import com.ververica.flink.training.provided.ShoppingCartWithCost;
import com.ververica.flink.training.solutions.CalcTotalCostMap;
import com.ververica.flink.training.solutions.RemoveUncompletedAndNotUSFilter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * The Review exercise from Ververica's Flink bootcamp training.
 *
 * <p>The goal of this exercise is to filter a data stream of eCommerce shopping cart records to
 * keep only records for completed transactions, and then calculate a total product weight, and
 * create separate records for each cart item.
 * </p>
 */
public class BootcampReview4Workflow {

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<CartItemWithShoppingCartInfo> resultSink;

    public BootcampReview4Workflow() {}

    public BootcampReview4Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampReview4Workflow setResultSink(Sink<CartItemWithShoppingCartInfo> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        cartStream
                .filter(new RemoveUncompletedAndNotUSFilter())
                .map(new CalcTotalCostMap())
                // TODO - use a FlatMapFunction to explode each ShoppingCartWithCost record into
                // multiple CartItemWithShoppingCartInfo records.
                // This inline FlatMap function is there to get the code to compile
                .flatMap(new FlatMapFunction<ShoppingCartWithCost, CartItemWithShoppingCartInfo>() {
                    @Override
                    public void flatMap(ShoppingCartWithCost value, Collector<CartItemWithShoppingCartInfo> out) throws Exception {

                    }
                })
                .sinkTo(resultSink);
    }
}
