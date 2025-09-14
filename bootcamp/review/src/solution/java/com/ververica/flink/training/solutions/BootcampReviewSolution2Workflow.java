/*
 * Copyright (C) 2024 Ververica, Inc, Inc. All rights reserved.
 * This file is part of the Ververica Academy training software.
 */

package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.exercises.BootcampReview1Workflow;
import com.ververica.flink.training.exercises.BootcampReview2Workflow;
import com.ververica.flink.training.provided.CartItemWithShoppingCartInfo;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;

/**
 * The Review exercise from Ververica's Flink bootcamp training.
 *
 * <p>The goal of this exercise is to filter a data stream of eCommerce shopping cart records to
 * keep only records for completed transactions in the US.
 */
public class BootcampReviewSolution2Workflow extends BootcampReview2Workflow {

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        cartStream
                .filter(r -> r.isTransactionCompleted())
                .filter(r -> r.getCountry().equals("US"))
                .sinkTo(resultSink);
    }
}
