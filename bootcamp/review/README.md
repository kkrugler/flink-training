# Lab: Bootcamp Review

## Introduction

This lab is the hands-on part of the "Apache Flink Bootcamp" training by Ververica.
Please follow the [Setup Instructions](../../README-Bootcamp.md#set-up-your-development-environment) first
and then continue reading here.

### The Flink Job

This simple Flink job reads eCommerce shopping cart activity data from a testing source that generates
fake records, does some transformations on them, and writes the results out to a test sink.

## Exercise 1

Modify the [BootcampReview1Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampReview1Workflow.java)
class to:

- Filter out any incomplete transactions, leaving only completed transactions.
- Filter out any transactions outside the US.
- Calculate a total price for each cart.
- Generate one record for each item in the cart.

To test, run the [BootcampReview1WorkflowTest](src/test/java/com/ververica/flink/training/exercises/BootcampReview1WorkflowTest.java)
in IntelliJ. The first time you run it, make sure you select the "test" task from the popup menu, not the
"testSolutions" task.

If you get stuck, classes for a working solution are located in the [solution](src/solution/java/com/ververica/flink/training/solutions/) directory.

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
