
# Lab: eCommerce Tables

## Introduction

This workflow uses the Flink Table API to join a stream of `TrimmedShoppingCart` records with a stream
of `CurrencyConvertRecord` records. 

## Exercise 1

You'll need to modify the
[BootcampTablesWorkflow](src/main/java/com/ververica/flink/training/exercises/BootcampTablesWorkflow.java)
code to use the Table APIs to define the tables, do the join, and generate the results. Hints are provided
in the code, as well as below.

To test, you should run the `BootcampTablesWorkflowTest.testWorkflow()` test.

### Hints

The required schemas for each of these stream's tables have been
provided for you as `TrimmedShoppingCartSchema` and `CurrencyConvertRecordTable`.


-----

[**Back to Bootcamp Overview**](../../README-bootcamp.md)
