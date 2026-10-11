package com.finstream.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JobConfigLoaderTest {

    @Test
    void testLoadDefaultConfig() {
        JobConfig config = JobConfigLoader.load();
        assertNotNull(config);

        // App
        assertEquals("FinStream-FDS-and-Parquet-Pipeline", config.getApp().getJobName());

        // Flink
        assertEquals(10000L, config.getFlink().getCheckpointIntervalMs());

        // Kafka
        assertEquals("localhost:9092", config.getKafka().getBootstrapServers());
        assertEquals("financial.transactions.raw", config.getKafka().getTopic());
        assertEquals("finstream-core-group", config.getKafka().getGroupId());
        assertEquals("http://localhost:8081", config.getKafka().getSchemaRegistryUrl());
        assertEquals("Kafka-ISO20022-Source", config.getKafka().getSourceName());
        assertEquals(5, config.getKafka().getWatermarkOutOfOrdernessSeconds());

        // FDS
        assertEquals("High-Amount-FDS-Filter", config.getFds().getFilterName());
        assertEquals("🚨 [FDS ALERT - 이상 고액 이체 탐지]", config.getFds().getAlertPrefix());
        assertEquals(10000000.0, config.getFds().getHighAmountThreshold());

        // Storage
        assertEquals("http://localhost:9000", config.getStorage().getS3Endpoint());
        assertEquals("admin", config.getStorage().getS3AccessKey());
        assertEquals("password123", config.getStorage().getS3SecretKey());
        assertTrue(config.getStorage().isS3PathStyleAccess());
        assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", config.getStorage().getS3aImpl());
        assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider", config.getStorage().getS3aCredentialsProvider());
        assertEquals("MinIO-Parquet-Sink", config.getStorage().getParquetSinkName());
        assertEquals("s3a://lakehouse/financial-transactions/", config.getStorage().getParquetSinkPath());
        assertEquals("'year='yyyy/'month='MM/'day='dd/'hour='HH", config.getStorage().getParquetBucketFormat());
    }

    @Test
    void testResolvePlaceholders() {
        String template = "url: ${CUSTOM_NON_EXISTENT_VAR:http://default-host:8080}";
        String resolved = JobConfigLoader.resolvePlaceholders(template);
        assertEquals("url: http://default-host:8080", resolved);
    }
}
