/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.training.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@DoNotChangeThis
public class SourceUtils {

    public static final Logger LOG = LoggerFactory.getLogger(SourceUtils.class);

    public static final int NUM_OF_MEASUREMENTS = 100_000;
    public static final int RANDOM_SEED = 1;
    public static final float FAILURE_RATE = 0.0001f;
    public static final List<Integer> IDLE_PARTITIONS = Arrays.asList(0, 4);

      /** Reads available locations from the 'cities#csv' resource file and returns them as a list. */
    public static List<String> readLocationsFromFile() {
        List<String> locations = new ArrayList<>();
        try (InputStream is = SourceUtils.class.getResourceAsStream("/cities.csv");
                BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            String city;
            while ((city = br.readLine()) != null) {
                locations.add(city);
            }
        } catch (IOException e) {
            LOG.error("Unable to read cities from file.", e);
            throw new RuntimeException(e);
        }
        return locations;
    }
}
