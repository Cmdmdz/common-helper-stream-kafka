package com.cmd.kafkahelper.stream.sender;

import com.cmd.kafkahelper.config.KafkaConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class StreamSenderFactory {

    private final KafkaConfig config;

    public StreamSender createKafkaStreamSender() {
        return createKafkaStreamSender(null);
    }


    public StreamSender createKafkaStreamSender(final Map<String, Object> producerConfig) {
        final Map<String, Object> props = new HashMap<>();

        if (config.getKafka() != null) {
            props.putAll(config.getKafka());
        }
        if (producerConfig != null) {
            props.putAll(producerConfig);
        }
        return new KafkaStreamSender(props);
    }



}
