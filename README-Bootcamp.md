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

# Apache Flink Bootcamp Exercises

Exercises that accompany the training content in the documentation.

## Table of Contents

[**Set up your development environment**](#set-up-your-development-environment)

1. [Software requirements](#software-requirements)
1. [Clone and build the flink-training project](#clone-and-build-the-flink-training-project)
1. [Import the flink-training project into your IDE](#import-the-flink-training-project-into-your-ide)

[**How to do the lab exercises**](#how-to-do-the-lab-exercises)

1. [Run and debug Flink programs in your IDE](#run-and-debug-flink-programs-in-your-ide)
1. [Exercises, tests, and solutions](#exercises-tests-and-solutions)

[**Lab exercises**](#lab-exercises)

## Set up your development environment

You will need to set up your environment in order to develop, debug, and execute solutions to the training exercises and examples.

### Software requirements

Flink supports Linux, OS X, and Windows as development environments for Flink programs and local execution. The following software is required for a Flink development setup and should be installed on your system:

- Git
- a JDK for Java 11 or Java 17 (a JRE is not sufficient; other versions of Java are currently not supported)
- an IDE for Java development with Gradle support
  - We recommend [IntelliJ](https://www.jetbrains.com/idea/), but [Eclipse](https://www.eclipse.org/downloads/) or 
    [Visual Studio Code](https://code.visualstudio.com/) (with the [Java extension pack](https://code.visualstudio.
    com/docs/java/java-tutorial)) can also be used so long as you stick to Java. 
  - The recent Eclipse comes with Java 21, make sure you configure the Gradle plugin to use Java 11 or Java 17.

> **:information_source: Note for Windows users:** The shell command examples provided in the training instructions are for UNIX systems.
> You may find it worthwhile to setup cygwin or WSL. For developing Flink jobs, Windows works reasonably well: you can run a Flink cluster on a single machine, submit jobs, run the webUI, and execute jobs in the IDE.

### Clone and build the flink-training project

This `flink-training` repository contains exercises, tests, and reference solutions for the programming exercises.

> **:information_source: Repository Layout:** This repository has several branches set up pointing to different Apache Flink versions, similarly to the [apache/flink](https://github.com/apache/flink) repository with:
> - a release branch for each minor version of Apache Flink, e.g. `release-1.19`, and
> - a `master` branch that points to the current Flink release (not `flink:master`!)
>
> If you want to work on a version other than the current Flink release, make sure to check out the appropriate branch.

Clone the `flink-training` repository from GitHub, navigate into the project repository, and build it:

```bash
git clone https://github.com/ververica/flink-training.git
cd flink-training
./gradlew test shadowJar
```

If this is your first time building it, you will end up downloading all of the dependencies for this Flink training project. This usually takes a few minutes, depending on the speed of your internet connection.

If all of the tests pass and the build is successful, you are off to a good start.

<details>
<summary><strong>:cn: Users in China: click here for instructions on using a local Maven mirror.</strong></summary>

If you are in China, we recommend configuring the Maven repository to use a mirror. You can do this by uncommenting this section in our [`build.gradle`](build.gradle) file:

```groovy
    repositories {
        // for access from China, you may need to uncomment this line
        maven { url 'https://maven.aliyun.com/repository/public/' }
        mavenCentral()
        maven {
            url "https://repository.apache.org/content/repositories/snapshots/"
            mavenContent {
                snapshotsOnly()
            }
        }
    }
```
</details>


### Import the flink-training project into your IDE

The project needs to be imported as a gradle project into your IDE.

Then you should be able to open [`ECommerceWindowing1WorkflowTest`](ecommerce/windowing/src/test/java/com/ververica/flink/training/exercises/ridecleansing/ECommerceWindowing1WorkflowTest.java) and run this test.

> **:information_source: Note for Eclipse users:** Several Gradle projects in this repo 
> depend on the Gradle project `common`. In order for Eclipse to detect the Gradle project dependencies correctly:
> You likely need to run the following command:
> 
> `cd flink-training; ./gradlew cleanEclipse cleanEclipseProject cleanEclipseClasspath eclipse`
> 
> Then, in the Gradle project that depends on `common`, set `Without test code` to `No` in the project dependence 
> setting. See the screenshot: 
> ![dependency-fix](images/project-dependency-fix-test-code.png)

## How to do the lab exercises

In the hands-on sessions, you will implement Flink programs using various Flink APIs.

The following steps guide you through the process of using the provided data streams, implementing your first Flink streaming program, and executing your program in your IDE.

We assume you have set up your development environment according to our [setup guide](#set-up-your-development-environment).

### Run and debug Flink programs in your IDE

Flink programs can be executed and debugged from within an IDE. This significantly eases the development process and provides an experience similar to working on any other Java application.

To start a Flink program in your IDE, run its `main()` method. Under the hood, the execution environment will start a local Flink instance within the same process. Hence, it is also possible to put breakpoints in your code and debug it.

If you have an IDE with this `flink-training` project imported, you can run (or debug) a streaming job by:

- opening the `org.apache.flink.training.examples.ridecount.RideCountExample` class in your IDE
- running (or debugging) the `main()` method of the `RideCountExample` class using your IDE

### Exercises, tests, and solutions

Each of these exercises include:
- an `...Exercise` class with most of the necessary boilerplate code for getting started
- a JUnit Test class (`...Test`) with a few tests for your implementation
- a `...Solution` class with a complete solution

There are Java versions of all the exercise, test, and solution classes. They can each be run from IntelliJ.

You can run exercises, solutions, and tests with the `gradlew` command.

To run tests:

```bash
./gradlew test
./gradlew :<subproject>:test
```

For Java exercises and solutions, we provide special tasks that can be listed with:

```bash
./gradlew printRunTasks
```

:point_down: Now you are ready to begin the lab exercises. :point_down:

## Lab exercises

TODO - update this list

1. [Filtering a Stream (Ride Cleansing)](ride-cleansing)
1. [Stateful Enrichment (Rides and Fares)](rides-and-fares)
1. [Windowed Analytics (Hourly Tips)](hourly-tips)
   - [Exercise](hourly-tips/README.md)
   - [Discussion](hourly-tips/DISCUSSION.md)
1. [`ProcessFunction` and Timers (Long Ride Alerts)](long-ride-alerts)
   - [Exercise](long-ride-alerts/README.md)
   - [Discussion](long-ride-alerts/DISCUSSION.md)
1. [Tuning & Troubleshooting Labs](troubleshooting)

