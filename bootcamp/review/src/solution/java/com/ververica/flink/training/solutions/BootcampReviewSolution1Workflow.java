/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.solutions;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;

import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.exercises.BootcampReview1Workflow;
import com.ververica.flink.training.provided.CartItemWithShoppingCartInfo;

/**
 * The Review exercise from Ververica's Flink bootcamp training.
 *
 * <p>The goal of this exercise is to filter a data stream of eCommerce shopping cart records to
 * keep only records for completed transactions in the US, and then calculate a total product cost,
 * and create separate records for each cart item
 */
public class BootcampReviewSolution1Workflow extends BootcampReview1Workflow {

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<CartItemWithShoppingCartInfo> resultSink;

    public BootcampReviewSolution1Workflow() {}

    public BootcampReviewSolution1Workflow setCartStream(
            DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampReviewSolution1Workflow setResultSink(
            Sink<CartItemWithShoppingCartInfo> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        cartStream
                .filter(new RemoveUncompletedAndNotUSFilter())
                .map(new CalcTotalCostMap())
                .flatMap(new ExplodeCartItemsFlatMap())
                .sinkTo(resultSink);
    }
}
