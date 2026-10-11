package com.finstream;

import com.finstream.config.JobConfig;
import com.finstream.config.JobConfigLoader;
import com.finstream.model.Pacs008Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class FdsStreamingJob {
    private static final Logger log = LoggerFactory.getLogger(FdsStreamingJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig jobConfig = JobConfigLoader.load(args);
        log.info("Starting Flink Streaming Job with config: {}", jobConfig.getApp().getJobName());

        Configuration config = new Configuration();
        // Flink S3 플러그인 (flink-s3-fs-hadoop) 전용 설정 키
        config.setString("s3.endpoint", jobConfig.getStorage().getS3Endpoint());
        config.setString("s3.access-key", jobConfig.getStorage().getS3AccessKey());
        config.setString("s3.secret-key", jobConfig.getStorage().getS3SecretKey());
        config.setString("s3.path.style.access", String.valueOf(jobConfig.getStorage().isS3PathStyleAccess()));

        // Hadoop S3A 호환 설정 키
        config.setString("fs.s3a.endpoint", jobConfig.getStorage().getS3Endpoint());
        config.setString("fs.s3a.access.key", jobConfig.getStorage().getS3AccessKey());
        config.setString("fs.s3a.secret.key", jobConfig.getStorage().getS3SecretKey());
        config.setString("fs.s3a.path.style.access", String.valueOf(jobConfig.getStorage().isS3PathStyleAccess()));
        config.setString("fs.s3a.impl", jobConfig.getStorage().getS3aImpl());
        config.setString("fs.s3a.aws.credentials.provider", jobConfig.getStorage().getS3aCredentialsProvider());

        // AWS SDK v1 시스템 프로퍼티 Fallback 설정
        System.setProperty("aws.accessKeyId", jobConfig.getStorage().getS3AccessKey());
        System.setProperty("aws.secretKey", jobConfig.getStorage().getS3SecretKey());

        // 로컬 실행 시 Flink 전역 FileSystem에 S3 설정 초기화
        FileSystem.initialize(config, null);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        // Exactly-Once를 위한 체크포인트 주기 설정
        env.enableCheckpointing(jobConfig.getFlink().getCheckpointIntervalMs());

        // 1. Kafka Source 설정 (Schema Registry 연동)
        KafkaSource<Pacs008Transaction> kafkaSource = KafkaSource.<Pacs008Transaction>builder()
                .setBootstrapServers(jobConfig.getKafka().getBootstrapServers())
                .setTopics(jobConfig.getKafka().getTopic())
                .setGroupId(jobConfig.getKafka().getGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                Pacs008Transaction.class,
                                jobConfig.getKafka().getSchemaRegistryUrl()))
                .build();

        // 2. 워터마크 전략 설정
        WatermarkStrategy<Pacs008Transaction> watermarkStrategy = WatermarkStrategy
                .<Pacs008Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(jobConfig.getKafka().getWatermarkOutOfOrdernessSeconds()))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStream<Pacs008Transaction> transactionStream = env.fromSource(
                kafkaSource,
                watermarkStrategy,
                jobConfig.getKafka().getSourceName());

        // 3. 실시간 FDS 필터링: 임계치 초과 이상 거래 로그 출력 (우선 스트림)
        transactionStream
                .filter(tx -> tx.getAmount() >= jobConfig.getFds().getHighAmountThreshold())
                .name(jobConfig.getFds().getFilterName())
                .print(jobConfig.getFds().getAlertPrefix());

        // 4. MinIO Parquet Sink: 정상 트랜잭션을 시간 단위 파티션으로 적재
        FileSink<Pacs008Transaction> parquetSink = FileSink
                .forBulkFormat(
                        new Path(jobConfig.getStorage().getParquetSinkPath()),
                        AvroParquetWriters.forSpecificRecord(Pacs008Transaction.class))
                .withBucketAssigner(new DateTimeBucketAssigner<>(jobConfig.getStorage().getParquetBucketFormat()))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        transactionStream.sinkTo(parquetSink).name(jobConfig.getStorage().getParquetSinkName());

        env.execute(jobConfig.getApp().getJobName());
    }
}