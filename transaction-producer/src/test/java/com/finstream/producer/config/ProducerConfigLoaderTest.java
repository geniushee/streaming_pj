package com.finstream.producer.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProducerConfigLoaderTest {

    @Test
    void testLoadDefaultConfig() {
        ProducerAppConfig config = ProducerConfigLoader.load();
        assertNotNull(config);

        // Kafka Producer
        assertEquals("localhost:9092", config.getKafka().getBootstrapServers());
        assertEquals("financial.transactions.raw", config.getKafka().getTopic());
        assertEquals("http://localhost:8081", config.getKafka().getSchemaRegistryUrl());
        assertEquals("all", config.getKafka().getAcks());
        assertTrue(config.getKafka().isEnableIdempotence());
        assertEquals(3, config.getKafka().getRetries());
        assertEquals(20, config.getKafka().getLingerMs());
        assertEquals(10, config.getKafka().getFlushInterval());

        // Generator
        assertEquals(1000L, config.getGenerator().getIntervalMs());
        assertEquals("KRW", config.getGenerator().getCurrency());
        assertEquals(10000000.0, config.getGenerator().getHighAmountThreshold());
        assertEquals(15, config.getGenerator().getHighAmountProbabilityPercent());
        assertEquals(10000000.0, config.getGenerator().getHighAmountBase());
        assertEquals(100000.0, config.getGenerator().getHighAmountStep());
        assertEquals(401, config.getGenerator().getHighAmountStepsCount());
        assertEquals(10000.0, config.getGenerator().getNormalAmountBase());
        assertEquals(10000.0, config.getGenerator().getNormalAmountStep());
        assertEquals(100, config.getGenerator().getNormalAmountStepsCount());

        assertNotNull(config.getGenerator().getAccounts());
        assertEquals(10, config.getGenerator().getAccounts().size());
        assertTrue(config.getGenerator().getAccounts().contains("ACCT-KR-1001"));
    }

    @Test
    void testResolvePlaceholders() {
        String template = "server: ${CUSTOM_NON_EXISTENT_VAR:my-kafka:9092}";
        String resolved = ProducerConfigLoader.resolvePlaceholders(template);
        assertEquals("server: my-kafka:9092", resolved);
    }
}
