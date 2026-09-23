package com.finstream;

import com.finstream.model.Pacs008Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.time.Duration;

public class FdsStreamingJob {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        // MinIO 연동용 S3A 설정
        config.setString("fs.s3a.endpoint", "http://localhost:9000");
        config.setString("fs.s3a.access.key", "admin");
        config.setString("fs.s3a.secret.key", "password123");
        config.setString("fs.s3a.path.style.access", "true");
        config.setString("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        // Exactly-Once를 위한 10초 주기 체크포인트 설정
        env.enableCheckpointing(10000);

        // 1. Kafka Source 설정 (Schema Registry 연동)
        KafkaSource<Pacs008Transaction> kafkaSource = KafkaSource.<Pacs008Transaction>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("financial.transactions.raw")
                .setGroupId("finstream-core-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                Pacs008Transaction.class,
                                "http://localhost:8081"))
                .build();

        // 2. 워터마크 전략 (5초 지연 도착 허용)
        WatermarkStrategy<Pacs008Transaction> watermarkStrategy = WatermarkStrategy
                .<Pacs008Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStream<Pacs008Transaction> transactionStream = env.fromSource(
                kafkaSource,
                watermarkStrategy,
                "Kafka-ISO20022-Source");

        // 3. 실시간 FDS 필터링: 1,000만 원 초과 이상 거래 로그 출력 (우선 스트림)
        transactionStream
                .filter(tx -> tx.getAmount() >= 10_000_000.0)
                .name("High-Amount-FDS-Filter")
                .print("🚨 [FDS ALERT - 이상 고액 이체 탐지]");

        // 4. MinIO Parquet Sink: 정상 트랜잭션을 시간 단위 파티션으로 적재
        FileSink<Pacs008Transaction> parquetSink = FileSink
                .forBulkFormat(
                        new Path("s3a://lakehouse/financial-transactions/"),
                        AvroParquetWriters.forSpecificRecord(Pacs008Transaction.class))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'year='yyyy/'month='MM/'day='dd/'hour='HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        transactionStream.sinkTo(parquetSink).name("MinIO-Parquet-Sink");

        env.execute("FinStream-FDS-and-Parquet-Pipeline");
    }
}