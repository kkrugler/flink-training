package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.FlinkClusterUtils;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.exercises.BootcampFailures1WorkflowTest;
import com.ververica.flink.training.exercises.BootcampFailuresConfig;
import com.ververica.flink.training.provided.TransactionalMemorySink;
import com.ververica.flink.training.provided.ShoppingCartFiles;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class BootcampFailuresSolution1WorkflowTest extends BootcampFailures1WorkflowTest {

    @Test
    public void testGettingCorrectResultsAfterFailure() throws Exception {
        testGettingCorrectResultsAfterFailure(new BootcampFailuresSolutionConfig());
    }

}