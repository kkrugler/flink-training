# Lab: Bootcamp Review

## Introduction

This lab is the hands-on part of the "Apache Flink Bootcamp" training by Ververica.
Please follow the [Setup Instructions](../../README-Bootcamp.md#set-up-your-development-environment) first
and then continue reading here.

### The Flink Job

This simple Flink job reads eCommerce shopping cart activity data from a testing source that generates
fake records, does some transformations on them, and writes the results out to a test sink.

If you get stuck, classes for a working solution are located in the [solution](src/solution/java/com/ververica/flink/training/solutions/) directory.

## Exercise 1

Modify the [BootcampReview1Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampReview1Workflow.java)
class to:

- Filter out any incomplete transactions, leaving only completed transactions.

To test, run the [BootcampReview1WorkflowTest](src/test/java/com/ververica/flink/training/exercises/BootcampReview1WorkflowTest.java)
in IntelliJ. The first time you run it, make sure you select the "test" task from the popup menu, not the
"testSolutions" task.

## Exercise 2

Modify the [BootcampReview2Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampReview2Workflow.java)
class to:

- Also filter out any transactions outside the US.

To test, run the [BootcampReview2WorkflowTest](src/test/java/com/ververica/flink/training/exercises/BootcampReview2WorkflowTest.java)
in IntelliJ. The first time you run it, make sure you select the "test" task from the popup menu, not the
"testSolutions" task.

## Exercise 3

Modify the [BootcampReview3Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampReview3Workflow.java)
class to:

- Calculate a total price for each cart, and save the results as
[ShoppingCartWithCost](src/provided/java/com/ververica/flink/training/provided/ShoppingCartWithCost.java) records.

To test, run the [BootcampReview3WorkflowTest](src/test/java/com/ververica/flink/training/exercises/BootcampReview3WorkflowTest.java)
in IntelliJ. The first time you run it, make sure you select the "test" task from the popup menu, not the
"testSolutions" task.

## Exercise 4

Modify the [BootcampReview4Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampReview4Workflow.java)
class to:

- Generate one record for each item in the cart, and save the results as
[CartItemWithShoppingCartInfo](src/provided/java/com/ververica/flink/training/provided/CartItemWithShoppingCartInfo.java).
You will need to use a `FlatMapFunction` to expand each ShoppingCartWithCost record into multiple CartItemWithShoppingCartInfo
records.

To test, run the [BootcampReview4WorkflowTest](src/test/java/com/ververica/flink/training/exercises/BootcampReview4WorkflowTest.java)
in IntelliJ. The first time you run it, make sure you select the "test" task from the popup menu, not the
"testSolutions" task.

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
