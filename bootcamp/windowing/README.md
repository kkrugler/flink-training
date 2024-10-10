# Lab: Bootcamp Windowing

## Introduction

This lab is the hands-on part of the "Apache Flink Bootcamp" training by Ververica. 
Please follow the [Setup Instructions](../../README-Bootcamp.md#set-up-your-development-environment) first
and then continue reading here.

### The Flink Job

This simple Flink job reads eCommerce shopping cart activity data from a testing source that generates
fake records. Records are filtered to only completed transactions, then grouped by country, divided
into tumbling one-minute windows, and then a count of total items in each cart is calculated.
The overall flow is depicted below:

```
+-------------------+     +-----------------------+     +-----------------+     +----------------------+     +--------------------+
| Fake Source for   |     |                       |     |                 |     |                      |     |                    |
| eCommerce records | --> | Watermarks/Timestamps | --> | Filtering       | --> | Windowed Aggregation | --> | Sink: NormalOutput |
|                   |     |                       |     |                 |     |                      |     |                    |
+-------------------+     +-----------------------+     +-----------------+     +----------------------+     +--------------------+
```

## Exercise 1

Modify the [BootcampWindowing1Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampWindowing1Workflow.java)
class to:

 - Filter out any `ShoppingCartRecord` that is not a completed transaction, via a
   Flink FilterFunction and the `ShoppingCartRecord.isTransactionCompleted()` method.
   Note you can use Java lambdas to easily implement simple filters like this.
 - Key the resulting stream by the record's country, then window the stream into
   tumbling (not sliding) windows of 1 minute.
 - Use a Flink `AggregationFunction` and a `ProcessWindowFunction` to calculate the
   total number of cart items for all the records found in each 1-minute/country
   window.
 - Generate a `KeyedWindowResult` record, where the key is the country, the time is
   the start of the window, and the count is the number of items.

## Exercise 2



-----

[**Back to Bootcamp Overview**](../../README-bootcamp.md)
