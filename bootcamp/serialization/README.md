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

# Lab: eCommerce Serialization

## Introduction

*  * 1. We strip down the incoming records and convert to a TrimmedShoppingCartRecord
*  * 2. TrimmedShoppingCartRecord is serializable as a POJO
*  * 3. Use KeyedProcessFunction to find duration, versus session window
*  * 4. Use simple structure for top two aggregation, versus priority queue


-----

[**Back to Bootcamp Overview**](../../README-bootcamp.md)
