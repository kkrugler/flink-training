package com.ververica.flink.training.solutions;

import com.ververica.flink.training.examples.BootcampExampleJob;
import org.junit.jupiter.api.Test;

public class BootcampExampleJobTest {

    // A short test to prevent Gradle builds from failing, due to lacking a solutions test.
    @Test
    public void runBootcampExampleJob() throws Exception {
        BootcampExampleJob.main(new String[] {"--numrecords", "1"});
    }
}
