package com.finstream.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobConfigLoader {
    private static final Logger log = LoggerFactory.getLogger(JobConfigLoader.class);
    private static final String DEFAULT_CONFIG_RESOURCE = "/application.yaml";
    private static final Pattern ENV_PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{([A-Za-z0-9_]+)(?::([^}]*))?\\}");

    public static JobConfig load() {
        return load(new String[0]);
    }

    public static JobConfig load(String[] args) {
        String configPath = resolveConfigPath(args);
        String rawContent = loadContent(configPath);
        String resolvedContent = resolvePlaceholders(rawContent);

        Yaml yaml = new Yaml();
        Map<String, Object> root = yaml.load(resolvedContent);
        if (root == null) {
            log.warn("Config file is empty. Using default JobConfig values.");
            return new JobConfig();
        }

        JobConfig config = buildJobConfig(root);
        log.info("Loaded JobConfig successfully: {}", config);
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
        try (InputStream in = JobConfigLoader.class.getResourceAsStream(DEFAULT_CONFIG_RESOURCE)) {
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
    private static JobConfig buildJobConfig(Map<String, Object> root) {
        JobConfig jobConfig = new JobConfig();

        // App
        Map<String, Object> appMap = (Map<String, Object>) root.get("app");
        if (appMap != null) {
            JobConfig.AppConfig appConfig = jobConfig.getApp();
            appConfig.setJobName(getString(appMap, "job-name", appConfig.getJobName()));
        }

        // Flink
        Map<String, Object> flinkMap = (Map<String, Object>) root.get("flink");
        if (flinkMap != null) {
            JobConfig.FlinkConfig flinkConfig = jobConfig.getFlink();
            Map<String, Object> checkpointMap = (Map<String, Object>) flinkMap.get("checkpoint");
            if (checkpointMap != null) {
                flinkConfig.setCheckpointIntervalMs(getLong(checkpointMap, "interval-ms", flinkConfig.getCheckpointIntervalMs()));
            }
        }

        // Kafka
        Map<String, Object> kafkaMap = (Map<String, Object>) root.get("kafka");
        if (kafkaMap != null) {
            JobConfig.KafkaConfig kafkaConfig = jobConfig.getKafka();
            kafkaConfig.setBootstrapServers(getString(kafkaMap, "bootstrap-servers", kafkaConfig.getBootstrapServers()));
            kafkaConfig.setTopic(getString(kafkaMap, "topic", kafkaConfig.getTopic()));
            kafkaConfig.setGroupId(getString(kafkaMap, "group-id", kafkaConfig.getGroupId()));
            kafkaConfig.setSchemaRegistryUrl(getString(kafkaMap, "schema-registry-url", kafkaConfig.getSchemaRegistryUrl()));
            kafkaConfig.setSourceName(getString(kafkaMap, "source-name", kafkaConfig.getSourceName()));

            Map<String, Object> watermarkMap = (Map<String, Object>) kafkaMap.get("watermark");
            if (watermarkMap != null) {
                kafkaConfig.setWatermarkOutOfOrdernessSeconds(getInt(watermarkMap, "out-of-orderness-seconds", kafkaConfig.getWatermarkOutOfOrdernessSeconds()));
            }
        }

        // FDS
        Map<String, Object> fdsMap = (Map<String, Object>) root.get("fds");
        if (fdsMap != null) {
            JobConfig.FdsConfig fdsConfig = jobConfig.getFds();
            fdsConfig.setFilterName(getString(fdsMap, "filter-name", fdsConfig.getFilterName()));
            fdsConfig.setAlertPrefix(getString(fdsMap, "alert-prefix", fdsConfig.getAlertPrefix()));
            fdsConfig.setHighAmountThreshold(getDouble(fdsMap, "high-amount-threshold", fdsConfig.getHighAmountThreshold()));
        }

        // Storage
        Map<String, Object> storageMap = (Map<String, Object>) root.get("storage");
        if (storageMap != null) {
            JobConfig.StorageConfig storageConfig = jobConfig.getStorage();

            Map<String, Object> s3Map = (Map<String, Object>) storageMap.get("s3");
            if (s3Map != null) {
                storageConfig.setS3Endpoint(getString(s3Map, "endpoint", storageConfig.getS3Endpoint()));
                storageConfig.setS3AccessKey(getString(s3Map, "access-key", storageConfig.getS3AccessKey()));
                storageConfig.setS3SecretKey(getString(s3Map, "secret-key", storageConfig.getS3SecretKey()));
                storageConfig.setS3PathStyleAccess(getBoolean(s3Map, "path-style-access", storageConfig.isS3PathStyleAccess()));
                storageConfig.setS3aImpl(getString(s3Map, "impl", storageConfig.getS3aImpl()));
                storageConfig.setS3aCredentialsProvider(getString(s3Map, "credentials-provider", storageConfig.getS3aCredentialsProvider()));
            }

            Map<String, Object> parquetMap = (Map<String, Object>) storageMap.get("parquet");
            if (parquetMap != null) {
                storageConfig.setParquetSinkName(getString(parquetMap, "sink-name", storageConfig.getParquetSinkName()));
                storageConfig.setParquetSinkPath(getString(parquetMap, "sink-path", storageConfig.getParquetSinkPath()));
                storageConfig.setParquetBucketFormat(getString(parquetMap, "bucket-format", storageConfig.getParquetBucketFormat()));
            }
        }

        return jobConfig;
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
