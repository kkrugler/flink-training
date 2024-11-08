package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.FakeParallelSource;
import com.ververica.flink.training.common.FlinkClusterUtils;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.common.ShoppingCartSource;
import com.ververica.flink.training.provided.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BootcampSortingSolutionWorkflowTest {

    @Test
    public void testWorkflow() throws Exception {
        // TODO - validate results.
        // We should use two reports, and a file sink, and verify that
        // what gets written matches our expectations.
        // We could generate 1000 random records, process them, then
        // sort by each report and verify we get the expected result.

    }

    @Test
    public void testPerformance() throws Exception {
        final long numRecords = 1_000_000;
        final int maxParallelism = 400;
        final int parallelism = 4;

        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 0);
        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(config, parallelism);
        env.setMaxParallelism(maxParallelism);

        ECommerceSource realSource = new ECommerceSource(numRecords);
        ECommerceEndSource endSource = new ECommerceEndSource(env.getParallelism());

        HybridSource<ECommerceRecord> realPlusEnd = HybridSource.builder(realSource)
                .addSource(endSource)
                .build();

        DataStream<ECommerceRecord> records = env.fromSource(realPlusEnd,
                WatermarkStrategy.noWatermarks(),
                "ECommerce Stream",
                TypeInformation.of(ECommerceRecord.class));

        new BootcampSortingSolutionWorkflow()
                .setCartStream(records)
                .setResultsSink(new DiscardingSink<>())
                .setBatchingParallelism(env.getParallelism())
                .setMaxParallelism(env.getMaxParallelism())
                .addReport(new ReportByCountrySortByShippingCost())
                .build();

        JobClient client = env.executeAsync("BootcampSortingSolutionWorkflow");

        while (!isDone(client) && (client.getJobStatus().get() != JobStatus.RUNNING)) {
            Thread.sleep(1L);
        }

        assertFalse(isDone(client));

        long startTime = System.currentTimeMillis();
        while (!isDone(client)) {
            Thread.sleep(1L);
        }

        long endTime = System.currentTimeMillis();
        System.out.format("Workflow time: %dms\n", endTime - startTime);
    }

    private static boolean isDone(JobClient client) {
        try {
            return client.getJobStatus().isDone();
        } catch (IllegalStateException e) {
            // MiniCluster throws this error when you're running a batch job, it's
            // completed, and then you call getJobStatus().
            return true;
        }
    }
}