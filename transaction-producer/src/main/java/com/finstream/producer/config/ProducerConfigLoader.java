package com.finstream.producer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProducerConfigLoader {
    private static final Logger log = LoggerFactory.getLogger(ProducerConfigLoader.class);
    private static final String DEFAULT_CONFIG_RESOURCE = "/application.yaml";
    private static final Pattern ENV_PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{([A-Za-z0-9_]+)(?::([^}]*))?\\}");

    public static ProducerAppConfig load() {
        return load(new String[0]);
    }

    public static ProducerAppConfig load(String[] args) {
        String configPath = resolveConfigPath(args);
        String rawContent = loadContent(configPath);
        String resolvedContent = resolvePlaceholders(rawContent);

        Yaml yaml = new Yaml();
        Map<String, Object> root = yaml.load(resolvedContent);
        if (root == null) {
            log.warn("Config file is empty. Using default ProducerAppConfig values.");
            return new ProducerAppConfig();
        }

        ProducerAppConfig config = buildConfig(root);
        log.info("Loaded ProducerAppConfig successfully: {}", config);
        return config;
    }

    private static String resolveConfigPath(String[] args) {
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("--config=")) {
                    return arg.substring("--config=".length());
                } else if ("-c".equals(arg) || "--config".equals(arg)) {
                    if (i + 1 < args.length) {
                        return args[i + 1];
                    }
                } else if (arg.endsWith(".yaml") || arg.endsWith(".yml")) {
                    return arg;
                }
            }
        }

        String sysProp = System.getProperty("config.file");
        if (sysProp != null && !sysProp.isBlank()) {
            return sysProp;
        }

        String envVar = System.getenv("CONFIG_FILE");
        if (envVar != null && !envVar.isBlank()) {
            return envVar;
        }

        return null;
    }

    private static String loadContent(String configPath) {
        if (configPath != null && !configPath.isBlank()) {
            File file = new File(configPath);
            if (!file.exists()) {
                throw new IllegalArgumentException("Config file not found at path: " + configPath);
            }
            log.info("Loading configuration from file: {}", file.getAbsolutePath());
            try (InputStream in = new FileInputStream(file)) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read config file from: " + configPath, e);
            }
        }

        log.info("Loading configuration from classpath resource: {}", DEFAULT_CONFIG_RESOURCE);
        try (InputStream in = ProducerConfigLoader.class.getResourceAsStream(DEFAULT_CONFIG_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Default config resource not found on classpath: " + DEFAULT_CONFIG_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read classpath config: " + DEFAULT_CONFIG_RESOURCE, e);
        }
    }

    static String resolvePlaceholders(String text) {
        if (text == null) {
            return "";
        }
        Matcher matcher = ENV_PLACEHOLDER_PATTERN.matcher(text);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String envVarName = matcher.group(1);
            String defaultValue = matcher.group(2);

            String envValue = System.getenv(envVarName);
            String replacement;
            if (envValue != null && !envValue.isEmpty()) {
                replacement = envValue;
            } else if (defaultValue != null) {
                replacement = defaultValue;
            } else {
                replacement = matcher.group(0);
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static ProducerAppConfig buildConfig(Map<String, Object> root) {
        ProducerAppConfig appConfig = new ProducerAppConfig();

        // Kafka
        Map<String, Object> kafkaMap = (Map<String, Object>) root.get("kafka");
        if (kafkaMap != null) {
            ProducerAppConfig.KafkaProducerConfig kafkaConfig = appConfig.getKafka();
            kafkaConfig.setBootstrapServers(getString(kafkaMap, "bootstrap-servers", kafkaConfig.getBootstrapServers()));
            kafkaConfig.setTopic(getString(kafkaMap, "topic", kafkaConfig.getTopic()));
            kafkaConfig.setSchemaRegistryUrl(getString(kafkaMap, "schema-registry-url", kafkaConfig.getSchemaRegistryUrl()));

            Map<String, Object> producerMap = (Map<String, Object>) kafkaMap.get("producer");
            if (producerMap != null) {
                kafkaConfig.setAcks(getString(producerMap, "acks", kafkaConfig.getAcks()));
                kafkaConfig.setEnableIdempotence(getBoolean(producerMap, "enable-idempotence", kafkaConfig.isEnableIdempotence()));
                kafkaConfig.setRetries(getInt(producerMap, "retries", kafkaConfig.getRetries()));
                kafkaConfig.setLingerMs(getInt(producerMap, "linger-ms", kafkaConfig.getLingerMs()));
                kafkaConfig.setFlushInterval(getInt(producerMap, "flush-interval", kafkaConfig.getFlushInterval()));
            }
        }

        // Generator
        Map<String, Object> generatorMap = (Map<String, Object>) root.get("generator");
        if (generatorMap != null) {
            ProducerAppConfig.GeneratorConfig generatorConfig = appConfig.getGenerator();
            generatorConfig.setIntervalMs(getLong(generatorMap, "interval-ms", generatorConfig.getIntervalMs()));
            generatorConfig.setCurrency(getString(generatorMap, "currency", generatorConfig.getCurrency()));
            generatorConfig.setHighAmountThreshold(getDouble(generatorMap, "high-amount-threshold", generatorConfig.getHighAmountThreshold()));
            generatorConfig.setHighAmountProbabilityPercent(getInt(generatorMap, "high-amount-probability-percent", generatorConfig.getHighAmountProbabilityPercent()));

            Map<String, Object> highAmountMap = (Map<String, Object>) generatorMap.get("high-amount");
            if (highAmountMap != null) {
                generatorConfig.setHighAmountBase(getDouble(highAmountMap, "base", generatorConfig.getHighAmountBase()));
                generatorConfig.setHighAmountStep(getDouble(highAmountMap, "step", generatorConfig.getHighAmountStep()));
                generatorConfig.setHighAmountStepsCount(getInt(highAmountMap, "steps-count", generatorConfig.getHighAmountStepsCount()));
            }

            Map<String, Object> normalAmountMap = (Map<String, Object>) generatorMap.get("normal-amount");
            if (normalAmountMap != null) {
                generatorConfig.setNormalAmountBase(getDouble(normalAmountMap, "base", generatorConfig.getNormalAmountBase()));
                generatorConfig.setNormalAmountStep(getDouble(normalAmountMap, "step", generatorConfig.getNormalAmountStep()));
                generatorConfig.setNormalAmountStepsCount(getInt(normalAmountMap, "steps-count", generatorConfig.getNormalAmountStepsCount()));
            }

            Object accountsObj = generatorMap.get("accounts");
            if (accountsObj instanceof List<?>) {
                List<String> list = new ArrayList<>();
                for (Object item : (List<?>) accountsObj) {
                    if (item != null) {
                        list.add(String.valueOf(item));
                    }
                }
                if (!list.isEmpty()) {
                    generatorConfig.setAccounts(list);
                }
            }
        }

        return appConfig;
    }

    private static String getString(Map<String, Object> map, String key, String defaultValue) {
        Object val = map.get(key);
        return val != null ? String.valueOf(val) : defaultValue;
    }

    private static int getInt(Map<String, Object> map, String key, int defaultValue) {
        Object val = map.get(key);
        if (val instanceof Number) {
            return ((Number) val).intValue();
        } else if (val instanceof String) {
            return Integer.parseInt((String) val);
        }
        return defaultValue;
    }

    private static long getLong(Map<String, Object> map, String key, long defaultValue) {
        Object val = map.get(key);
        if (val instanceof Number) {
            return ((Number) val).longValue();
        } else if (val instanceof String) {
            return Long.parseLong((String) val);
        }
        return defaultValue;
    }

    private static double getDouble(Map<String, Object> map, String key, double defaultValue) {
        Object val = map.get(key);
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        } else if (val instanceof String) {
            return Double.parseDouble((String) val);
        }
        return defaultValue;
    }

    private static boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
        Object val = map.get(key);
        if (val instanceof Boolean) {
            return (Boolean) val;
        } else if (val instanceof String) {
            return Boolean.parseBoolean((String) val);
        }
        return defaultValue;
    }
}
