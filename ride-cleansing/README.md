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

# Lab: Filtering a Stream (Ride Cleansing)

If you haven't already done so, you'll need to first [setup your Flink development environment](../README-Developer.md). See [How to do the Labs](../README-Developer.md#how-to-do-the-lab-exercises) for an overall introduction to these exercises.

The task of the "Taxi Ride Cleansing" exercise is to cleanse a stream of TaxiRide events by removing events that start or end outside of New York City.

The `GeoUtils` utility class provides a static method `isInNYC(float lon, float lat)` to check if a location is within the NYC area.

### Input Data

This exercise is based on a stream of `TaxiRide` events, as described in [Using the Taxi Data Streams](../README-Developer.md#use-the-taxi-data-streams).

### Expected Output

The result of the exercise should be a `DataStream<TaxiRide>` that only contains events of taxi rides which both start and end in the New York City area as defined by `GeoUtils.isInNYC()`.

The resulting stream should be printed to standard out.

## Getting Started

### Exercise Classes

This exercise uses these classes:

[`org.apache.flink.training.exercises.ridecleansing.RideCleansingExercise`](src/main/java/org/apache/flink/training/exercises/ridecleansing/RideCleansingExercise.java)

### Tests

- Unit tests: [`org.apache.flink.training.exercises.ridecleansing.RideCleansingUnitTest`](src/test/java/org/apache/flink/training/exercises/ridecleansing/RideCleansingUnitTest.java)
- Integration tests: [`org.apache.flink.training.exercises.ridecleansing.RideCleansingIntegrationTest`](src/test/java/org/apache/flink/training/exercises/ridecleansing/RideCleansingIntegrationTest.java)

Like most of these exercises, at some point the `RideCleansingExercise` class throws an exception

```java
throw new MissingSolutionException();
```

Once you remove this line, the test will fail until you provide a working solution. You might want to first try something clearly broken, such as

```java
return false;
```

in order to verify that the test does indeed fail when you make a mistake, and then work on implementing a proper solution.

## Implementation Hints

<details>
<summary><strong>Filtering Events</strong></summary>

Flink's DataStream API features a `DataStream.filter(FilterFunction)` transformation to filter events from a data stream. The `GeoUtils.isInNYC()` function can be called within a `FilterFunction` to check if a location is in the New York City area. Your filter function should check both the starting and ending locations of each ride.
</details>

## Documentation

- [DataStream API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview)
- [Flink JavaDocs](https://nightlies.apache.org/flink/flink-docs-stable/api/java)

## Reference Solutions

Reference solutions are available in this project:

[`org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution`](src/solution/java/org/apache/flink/training/solutions/ridecleansing/RideCleansingSolution.java)

-----

[**Back to Labs Overview**](../README-Developer.md#lab-exercises)
