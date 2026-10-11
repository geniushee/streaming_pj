package com.finstream.producer;

import com.finstream.model.Pacs008Transaction;
import com.finstream.producer.config.ProducerAppConfig;
import com.finstream.producer.config.ProducerConfigLoader;
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

    private static final Random RANDOM = new Random();
    private static final NumberFormat CURRENCY_FORMAT = NumberFormat.getNumberInstance(Locale.KOREA);

    public static void main(String[] args) {
        ProducerAppConfig appConfig = ProducerConfigLoader.load(args);
        ProducerAppConfig.KafkaProducerConfig kafkaConfig = appConfig.getKafka();
        ProducerAppConfig.GeneratorConfig genConfig = appConfig.getGenerator();

        log.info("Starting MockTransactionProducer with topic: {}, servers: {}",
                kafkaConfig.getTopic(), kafkaConfig.getBootstrapServers());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", kafkaConfig.getSchemaRegistryUrl());

        // 금융 트랜잭션 전송 신뢰성 보장 옵션
        props.put(ProducerConfig.ACKS_CONFIG, kafkaConfig.getAcks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(kafkaConfig.isEnableIdempotence()));
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaConfig.getRetries());
        props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaConfig.getLingerMs());

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
                Pacs008Transaction transaction = generateMockTransaction(genConfig);
                String partitionKey = transaction.getSenderAccount();

                ProducerRecord<String, Pacs008Transaction> record =
                        new ProducerRecord<>(kafkaConfig.getTopic(), partitionKey, transaction);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            log.error("Failed to send transaction [txId={}]: {}", transaction.getTxId(), exception.getMessage());
                        } else {
                            String formattedAmount = CURRENCY_FORMAT.format(transaction.getAmount());
                            if (transaction.getAmount() >= genConfig.getHighAmountThreshold()) {
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
                if (count % kafkaConfig.getFlushInterval() == 0) {
                    producer.flush();
                }

                Thread.sleep(genConfig.getIntervalMs());
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

    private static Pacs008Transaction generateMockTransaction(ProducerAppConfig.GeneratorConfig genConfig) {
        List<String> accounts = genConfig.getAccounts();
        int senderIdx = RANDOM.nextInt(accounts.size());
        int receiverIdx;
        do {
            receiverIdx = RANDOM.nextInt(accounts.size());
        } while (receiverIdx == senderIdx);

        String sender = accounts.get(senderIdx);
        String receiver = accounts.get(receiverIdx);

        // 지정된 확률로 FDS 탐지 대상 고액 거래 발생
        boolean isHighAmount = RANDOM.nextInt(100) < genConfig.getHighAmountProbabilityPercent();
        double amount;
        if (isHighAmount) {
            amount = genConfig.getHighAmountBase() + (RANDOM.nextInt(genConfig.getHighAmountStepsCount()) * genConfig.getHighAmountStep());
        } else {
            amount = genConfig.getNormalAmountBase() + (RANDOM.nextInt(genConfig.getNormalAmountStepsCount()) * genConfig.getNormalAmountStep());
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
                .setCurrency(genConfig.getCurrency())
                .setIdempotencyKey(idempotencyKey)
                .build();
    }
}

