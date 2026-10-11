package com.finstream.producer.config;

import java.io.Serializable;
import java.util.List;

public class ProducerAppConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private KafkaProducerConfig kafka = new KafkaProducerConfig();
    private GeneratorConfig generator = new GeneratorConfig();

    public KafkaProducerConfig getKafka() {
        return kafka;
    }

    public void setKafka(KafkaProducerConfig kafka) {
        this.kafka = kafka;
    }

    public GeneratorConfig getGenerator() {
        return generator;
    }

    public void setGenerator(GeneratorConfig generator) {
        this.generator = generator;
    }

    @Override
    public String toString() {
        return "ProducerAppConfig{" +
                "kafka=" + kafka +
                ", generator=" + generator +
                '}';
    }

    public static class KafkaProducerConfig implements Serializable {
        private String bootstrapServers = "localhost:9092";
        private String topic = "financial.transactions.raw";
        private String schemaRegistryUrl = "http://localhost:8081";
        private String acks = "all";
        private boolean enableIdempotence = true;
        private int retries = 3;
        private int lingerMs = 20;
        private int flushInterval = 10;

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

        public String getSchemaRegistryUrl() {
            return schemaRegistryUrl;
        }

        public void setSchemaRegistryUrl(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
        }

        public String getAcks() {
            return acks;
        }

        public void setAcks(String acks) {
            this.acks = acks;
        }

        public boolean isEnableIdempotence() {
            return enableIdempotence;
        }

        public void setEnableIdempotence(boolean enableIdempotence) {
            this.enableIdempotence = enableIdempotence;
        }

        public int getRetries() {
            return retries;
        }

        public void setRetries(int retries) {
            this.retries = retries;
        }

        public int getLingerMs() {
            return lingerMs;
        }

        public void setLingerMs(int lingerMs) {
            this.lingerMs = lingerMs;
        }

        public int getFlushInterval() {
            return flushInterval;
        }

        public void setFlushInterval(int flushInterval) {
            this.flushInterval = flushInterval;
        }

        @Override
        public String toString() {
            return "KafkaProducerConfig{" +
                    "bootstrapServers='" + bootstrapServers + '\'' +
                    ", topic='" + topic + '\'' +
                    ", schemaRegistryUrl='" + schemaRegistryUrl + '\'' +
                    ", acks='" + acks + '\'' +
                    ", enableIdempotence=" + enableIdempotence +
                    ", retries=" + retries +
                    ", lingerMs=" + lingerMs +
                    ", flushInterval=" + flushInterval +
                    '}';
        }
    }

    public static class GeneratorConfig implements Serializable {
        private long intervalMs = 1000L;
        private String currency = "KRW";
        private double highAmountThreshold = 10000000.0;
        private int highAmountProbabilityPercent = 15;

        private double highAmountBase = 10000000.0;
        private double highAmountStep = 100000.0;
        private int highAmountStepsCount = 401;

        private double normalAmountBase = 10000.0;
        private double normalAmountStep = 10000.0;
        private int normalAmountStepsCount = 100;

        private List<String> accounts = List.of(
                "ACCT-KR-1001", "ACCT-KR-1002", "ACCT-KR-1003", "ACCT-KR-1004", "ACCT-KR-1005",
                "ACCT-KR-1006", "ACCT-KR-1007", "ACCT-KR-1008", "ACCT-KR-1009", "ACCT-KR-1010"
        );

        public long getIntervalMs() {
            return intervalMs;
        }

        public void setIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        public String getCurrency() {
            return currency;
        }

        public void setCurrency(String currency) {
            this.currency = currency;
        }

        public double getHighAmountThreshold() {
            return highAmountThreshold;
        }

        public void setHighAmountThreshold(double highAmountThreshold) {
            this.highAmountThreshold = highAmountThreshold;
        }

        public int getHighAmountProbabilityPercent() {
            return highAmountProbabilityPercent;
        }

        public void setHighAmountProbabilityPercent(int highAmountProbabilityPercent) {
            this.highAmountProbabilityPercent = highAmountProbabilityPercent;
        }

        public double getHighAmountBase() {
            return highAmountBase;
        }

        public void setHighAmountBase(double highAmountBase) {
            this.highAmountBase = highAmountBase;
        }

        public double getHighAmountStep() {
            return highAmountStep;
        }

        public void setHighAmountStep(double highAmountStep) {
            this.highAmountStep = highAmountStep;
        }

        public int getHighAmountStepsCount() {
            return highAmountStepsCount;
        }

        public void setHighAmountStepsCount(int highAmountStepsCount) {
            this.highAmountStepsCount = highAmountStepsCount;
        }

        public double getNormalAmountBase() {
            return normalAmountBase;
        }

        public void setNormalAmountBase(double normalAmountBase) {
            this.normalAmountBase = normalAmountBase;
        }

        public double getNormalAmountStep() {
            return normalAmountStep;
        }

        public void setNormalAmountStep(double normalAmountStep) {
            this.normalAmountStep = normalAmountStep;
        }

        public int getNormalAmountStepsCount() {
            return normalAmountStepsCount;
        }

        public void setNormalAmountStepsCount(int normalAmountStepsCount) {
            this.normalAmountStepsCount = normalAmountStepsCount;
        }

        public List<String> getAccounts() {
            return accounts;
        }

        public void setAccounts(List<String> accounts) {
            this.accounts = accounts;
        }

        @Override
        public String toString() {
            return "GeneratorConfig{" +
                    "intervalMs=" + intervalMs +
                    ", currency='" + currency + '\'' +
                    ", highAmountThreshold=" + highAmountThreshold +
                    ", highAmountProbabilityPercent=" + highAmountProbabilityPercent +
                    ", accountsCount=" + (accounts != null ? accounts.size() : 0) +
                    '}';
        }
    }
}
