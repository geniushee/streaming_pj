package com.finstream.config;

import java.io.Serializable;

public class JobConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private AppConfig app = new AppConfig();
    private FlinkConfig flink = new FlinkConfig();
    private KafkaConfig kafka = new KafkaConfig();
    private FdsConfig fds = new FdsConfig();
    private StorageConfig storage = new StorageConfig();

    public AppConfig getApp() {
        return app;
    }

    public void setApp(AppConfig app) {
        this.app = app;
    }

    public FlinkConfig getFlink() {
        return flink;
    }

    public void setFlink(FlinkConfig flink) {
        this.flink = flink;
    }

    public KafkaConfig getKafka() {
        return kafka;
    }

    public void setKafka(KafkaConfig kafka) {
        this.kafka = kafka;
    }

    public FdsConfig getFds() {
        return fds;
    }

    public void setFds(FdsConfig fds) {
        this.fds = fds;
    }

    public StorageConfig getStorage() {
        return storage;
    }

    public void setStorage(StorageConfig storage) {
        this.storage = storage;
    }

    @Override
    public String toString() {
        return "JobConfig{" +
                "app=" + app +
                ", flink=" + flink +
                ", kafka=" + kafka +
                ", fds=" + fds +
                ", storage=" + storage +
                '}';
    }

    public static class AppConfig implements Serializable {
        private String jobName = "FinStream-FDS-and-Parquet-Pipeline";

        public String getJobName() {
            return jobName;
        }

        public void setJobName(String jobName) {
            this.jobName = jobName;
        }

        @Override
        public String toString() {
            return "AppConfig{jobName='" + jobName + "'}";
        }
    }

    public static class FlinkConfig implements Serializable {
        private long checkpointIntervalMs = 10000L;

        public long getCheckpointIntervalMs() {
            return checkpointIntervalMs;
        }

        public void setCheckpointIntervalMs(long checkpointIntervalMs) {
            this.checkpointIntervalMs = checkpointIntervalMs;
        }

        @Override
        public String toString() {
            return "FlinkConfig{checkpointIntervalMs=" + checkpointIntervalMs + "}";
        }
    }

    public static class KafkaConfig implements Serializable {
        private String bootstrapServers = "localhost:9092";
        private String topic = "financial.transactions.raw";
        private String groupId = "finstream-core-group";
        private String schemaRegistryUrl = "http://localhost:8081";
        private String sourceName = "Kafka-ISO20022-Source";
        private int watermarkOutOfOrdernessSeconds = 5;

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getSchemaRegistryUrl() {
            return schemaRegistryUrl;
        }

        public void setSchemaRegistryUrl(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
        }

        public String getSourceName() {
            return sourceName;
        }

        public void setSourceName(String sourceName) {
            this.sourceName = sourceName;
        }

        public int getWatermarkOutOfOrdernessSeconds() {
            return watermarkOutOfOrdernessSeconds;
        }

        public void setWatermarkOutOfOrdernessSeconds(int watermarkOutOfOrdernessSeconds) {
            this.watermarkOutOfOrdernessSeconds = watermarkOutOfOrdernessSeconds;
        }

        @Override
        public String toString() {
            return "KafkaConfig{" +
                    "bootstrapServers='" + bootstrapServers + '\'' +
                    ", topic='" + topic + '\'' +
                    ", groupId='" + groupId + '\'' +
                    ", schemaRegistryUrl='" + schemaRegistryUrl + '\'' +
                    ", sourceName='" + sourceName + '\'' +
                    ", watermarkOutOfOrdernessSeconds=" + watermarkOutOfOrdernessSeconds +
                    '}';
        }
    }

    public static class FdsConfig implements Serializable {
        private String filterName = "High-Amount-FDS-Filter";
        private String alertPrefix = "🚨 [FDS ALERT - 이상 고액 이체 탐지]";
        private double highAmountThreshold = 10000000.0;

        public String getFilterName() {
            return filterName;
        }

        public void setFilterName(String filterName) {
            this.filterName = filterName;
        }

        public String getAlertPrefix() {
            return alertPrefix;
        }

        public void setAlertPrefix(String alertPrefix) {
            this.alertPrefix = alertPrefix;
        }

        public double getHighAmountThreshold() {
            return highAmountThreshold;
        }

        public void setHighAmountThreshold(double highAmountThreshold) {
            this.highAmountThreshold = highAmountThreshold;
        }

        @Override
        public String toString() {
            return "FdsConfig{" +
                    "filterName='" + filterName + '\'' +
                    ", alertPrefix='" + alertPrefix + '\'' +
                    ", highAmountThreshold=" + highAmountThreshold +
                    '}';
        }
    }

    public static class StorageConfig implements Serializable {
        private String s3Endpoint = "http://localhost:9000";
        private String s3AccessKey = "admin";
        private String s3SecretKey = "password123";
        private boolean s3PathStyleAccess = true;
        private String s3aImpl = "org.apache.hadoop.fs.s3a.S3AFileSystem";
        private String s3aCredentialsProvider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
        private String parquetSinkName = "MinIO-Parquet-Sink";
        private String parquetSinkPath = "s3a://lakehouse/financial-transactions/";
        private String parquetBucketFormat = "'year='yyyy/'month='MM/'day='dd/'hour='HH";

        public String getS3Endpoint() {
            return s3Endpoint;
        }

        public void setS3Endpoint(String s3Endpoint) {
            this.s3Endpoint = s3Endpoint;
        }

        public String getS3AccessKey() {
            return s3AccessKey;
        }

        public void setS3AccessKey(String s3AccessKey) {
            this.s3AccessKey = s3AccessKey;
        }

        public String getS3SecretKey() {
            return s3SecretKey;
        }

        public void setS3SecretKey(String s3SecretKey) {
            this.s3SecretKey = s3SecretKey;
        }

        public boolean isS3PathStyleAccess() {
            return s3PathStyleAccess;
        }

        public void setS3PathStyleAccess(boolean s3PathStyleAccess) {
            this.s3PathStyleAccess = s3PathStyleAccess;
        }

        public String getS3aImpl() {
            return s3aImpl;
        }

        public void setS3aImpl(String s3aImpl) {
            this.s3aImpl = s3aImpl;
        }

        public String getS3aCredentialsProvider() {
            return s3aCredentialsProvider;
        }

        public void setS3aCredentialsProvider(String s3aCredentialsProvider) {
            this.s3aCredentialsProvider = s3aCredentialsProvider;
        }

        public String getParquetSinkName() {
            return parquetSinkName;
        }

        public void setParquetSinkName(String parquetSinkName) {
            this.parquetSinkName = parquetSinkName;
        }

        public String getParquetSinkPath() {
            return parquetSinkPath;
        }

        public void setParquetSinkPath(String parquetSinkPath) {
            this.parquetSinkPath = parquetSinkPath;
        }

        public String getParquetBucketFormat() {
            return parquetBucketFormat;
        }

        public void setParquetBucketFormat(String parquetBucketFormat) {
            this.parquetBucketFormat = parquetBucketFormat;
        }

        @Override
        public String toString() {
            return "StorageConfig{" +
                    "s3Endpoint='" + s3Endpoint + '\'' +
                    ", s3AccessKey='" + s3AccessKey + '\'' +
                    ", s3SecretKey='****'" +
                    ", s3PathStyleAccess=" + s3PathStyleAccess +
                    ", s3aImpl='" + s3aImpl + '\'' +
                    ", s3aCredentialsProvider='" + s3aCredentialsProvider + '\'' +
                    ", parquetSinkName='" + parquetSinkName + '\'' +
                    ", parquetSinkPath='" + parquetSinkPath + '\'' +
                    ", parquetBucketFormat='" + parquetBucketFormat + '\'' +
                    '}';
        }
    }
}
