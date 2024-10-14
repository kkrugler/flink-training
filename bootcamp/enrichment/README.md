<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lab: eCommerce Enrichment

## Introduction

The goal of this lab is to use two different techniques to enrich a stream
of `ShoppingCartRecord` records with additional information.

## Exercise 1

Take a look at the [BootcampEnrichment1Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampEnrichment1Workflow.java)
class to see how the rather simple workflow calls a custom map function called
[CalcTotalUSDollarPriceFunction](src/main/java/com/ververica/flink/training/exercises/CalcTotalUSDollarPriceFunction.java)
to calculate the total value of all items in the shopping cart, normalized to
US dollars.

In order to do this, it makes use of a provided "Currency Exchange Rate" API
that is provided by the [CurrencyRateAPI](src/provided/java/com/ververica/flink/training/provided/CurrencyRateAPI.java)
class. This simulates an external service, one that would typically be called via
HTTP requests.

Modify the `CalcTotalUSDollarPriceFunction` to use this API to perform the
API request and calculation. You can test this via either running the
`BootcampEnrichment1WorkflowTest.testAddingUSDEquivalent()` test (integration),
or the 


Modify the 

This lab provides the basis of the hands-on part of the "Apache Flink Bootcamp"
training by Ververica. Please follow the [Setup Instructions](../../README.md#setup-your-development-environment) first
and then continue reading here.

TODO - fill out

-----

[**Back to eCommerce Overview**](../README.md)
