/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.exercises;

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

import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.CartItemWithShoppingCartInfo;

/**
 * The Review exercise from Ververica's Flink bootcamp training.
 *
 * <p>The goal of this exercise is to filter a data stream of eCommerce shopping cart records to
 * keep only records for completed transactions, and then calculate a total product weight, and
 * create separate records for each cart item
 */
public class BootcampReview1Workflow {

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<CartItemWithShoppingCartInfo> resultSink;

    public BootcampReview1Workflow() {}

    public BootcampReview1Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampReview1Workflow setResultSink(Sink<CartItemWithShoppingCartInfo> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        cartStream.keyBy(r -> r.getCountry())
                .process(new KeyedProcessFunction<String, ShoppingCartRecord, Tuple2<String, Integer>>() {
                    private transient ValueState<Integer> state;
                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        ValueStateDescriptor vsd = new ValueStateDescriptor("state", Integer.class);
                        state = getRuntimeContext().getState(vsd);
                    }

                    @Override
                    public void processElement(ShoppingCartRecord value, Context ctx, Collector<Tuple2<String,Integer>> out) throws Exception {
                        Integer v = state.value();
                        if (v == null) {
                            v = 1;
                        } else {
                            v = v + 1;
                        }
                        state.update(v);
                        out.collect(Tuple2.of(ctx.getCurrentKey(), v));
                    }
                })
                        .sinkTo(new PrintSink<>());

        // TODO - filter out transactions out that are NOT completed.
        // TODO - Implement this as a filter function.

        // TODO - calculate a total weight, by summing the weights of all items.
        // TODO - output this as a ShoppingCartWithCost record
        // TODO - Implement this as a map function.

        // TODO - generate a separate CartItemWithShoppingCartInfo for each item in the cart
        // TODO - Implement this as a flatmap function.

        // ==================================================================
        // Placeholder to get code to compile.
        // ==================================================================

        // TODO - replace this with a .sinkTo(resultSink);
        cartStream.map(r -> new CartItemWithShoppingCartInfo(r.getItems().get(0), "", 0)).sinkTo(resultSink);
    }
}
