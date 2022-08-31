package com.cmd.kafkahelper.stream.receiver;

import com.cmd.kafkahelper.config.KafkaConfig;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class StreamReceiverFactory {
    private static final String DEFAULT_AUTO_OFFSET_RESET = "latest";

    @Value("${spring.application.name:}")
    private String applicationName;

    private final KafkaConfig config;

    public <T> StreamReceiver<T> createKafkaStreamReceiver(final String topic, final Class<T> valueType) {
        return this.createKafkaStreamReceiver(topic, valueType, null);
    }

    public <T> StreamReceiver<T> createKafkaStreamReceiver(final String topic, final Class<T> valueType, final Map<String, Object> consumerConfig) {
        final Map<String, Object> props = new HashMap<>();
        if (config.getKafka() != null) {
            props.putAll(config.getKafka());
        }
        if (consumerConfig != null) {
            props.putAll(consumerConfig);
        }
        props.computeIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, key -> applicationName + UUID.randomUUID().toString().substring(23));
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET);
        return new KafkaStreamReceiver<>(props, topic, valueType);
    }


}
