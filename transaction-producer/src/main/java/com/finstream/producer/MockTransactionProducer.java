package com.finstream.producer;

import com.finstream.model.Pacs008Transaction;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class MockTransactionProducer {
    private static final Logger log = LoggerFactory.getLogger(MockTransactionProducer.class);

    private static final String DEFAULT_TOPIC = "financial.transactions.raw";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static final List<String> ACCOUNTS = List.of(
            "ACCT-KR-1001", "ACCT-KR-1002", "ACCT-KR-1003", "ACCT-KR-1004", "ACCT-KR-1005",
            "ACCT-KR-1006", "ACCT-KR-1007", "ACCT-KR-1008", "ACCT-KR-1009", "ACCT-KR-1010"
    );

    private static final Random RANDOM = new Random();
    private static final NumberFormat CURRENCY_FORMAT = NumberFormat.getNumberInstance(Locale.KOREA);

    public static void main(String[] args) {
        String topic = System.getenv().getOrDefault("TOPIC_NAME", DEFAULT_TOPIC);
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL);

        log.info("Starting MockTransactionProducer...");
        log.info("Target Topic: {}", topic);
        log.info("Bootstrap Servers: {}", bootstrapServers);
        log.info("Schema Registry: {}", schemaRegistryUrl);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);

        // 금융 트랜잭션 전송 신뢰성 보장 옵션
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        KafkaProducer<String, Pacs008Transaction> producer = new KafkaProducer<>(props);
        AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown requested. Closing Kafka Producer...");
            running.set(false);
            producer.flush();
            producer.close();
            log.info("Kafka Producer successfully closed.");
        }));

        long count = 0;
        try {
            while (running.get()) {
                Pacs008Transaction transaction = generateMockTransaction();
                String partitionKey = transaction.getSenderAccount();

                ProducerRecord<String, Pacs008Transaction> record =
                        new ProducerRecord<>(topic, partitionKey, transaction);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            log.error("Failed to send transaction [txId={}]: {}", transaction.getTxId(), exception.getMessage());
                        } else {
                            String formattedAmount = CURRENCY_FORMAT.format(transaction.getAmount());
                            if (transaction.getAmount() >= 10_000_000.0) {
                                log.warn("🚨 [PRODUCED - FDS TARGET] txId={}, sender={}, receiver={}, amount={} {}, partition={}, offset={}",
                                        transaction.getTxId(), transaction.getSenderAccount(), transaction.getReceiverAccount(),
                                        formattedAmount, transaction.getCurrency(), metadata.partition(), metadata.offset());
                            } else {
                                log.info("✅ [PRODUCED - NORMAL] txId={}, sender={}, receiver={}, amount={} {}, partition={}, offset={}",
                                        transaction.getTxId(), transaction.getSenderAccount(), transaction.getReceiverAccount(),
                                        formattedAmount, transaction.getCurrency(), metadata.partition(), metadata.offset());
                            }
                        }
                    }
                });

                count++;
                if (count % 10 == 0) {
                    producer.flush();
                }

                // 1초 주기로 전송 (터미널 모니터링 최적화)
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            log.info("Producer loop interrupted.");
            Thread.currentThread().interrupt();
        } finally {
            if (running.get()) {
                producer.flush();
                producer.close();
            }
        }
    }

    private static Pacs008Transaction generateMockTransaction() {
        int senderIdx = RANDOM.nextInt(ACCOUNTS.size());
        int receiverIdx;
        do {
            receiverIdx = RANDOM.nextInt(ACCOUNTS.size());
        } while (receiverIdx == senderIdx);

        String sender = ACCOUNTS.get(senderIdx);
        String receiver = ACCOUNTS.get(receiverIdx);

        // 15% 확률로 1,000만 원 이상의 FDS 탐지 대상 고액 거래 발생
        boolean isHighAmount = RANDOM.nextInt(100) < 15;
        double amount;
        if (isHighAmount) {
            // 1,000만 원 ~ 5,000만 원 (10만 원 단위)
            amount = 10_000_000.0 + (RANDOM.nextInt(401) * 100_000.0);
        } else {
            // 1만 원 ~ 100만 원 (1만 원 단위)
            amount = 10_000.0 + (RANDOM.nextInt(100) * 10_000.0);
        }

        String txId = UUID.randomUUID().toString();
        String idempotencyKey = "IDEMP-" + UUID.randomUUID().toString().substring(0, 8);
        long timestamp = System.currentTimeMillis();

        return Pacs008Transaction.newBuilder()
                .setTxId(txId)
                .setTimestamp(timestamp)
                .setSenderAccount(sender)
                .setReceiverAccount(receiver)
                .setAmount(amount)
                .setCurrency("KRW")
                .setIdempotencyKey(idempotencyKey)
                .build();
    }
}

