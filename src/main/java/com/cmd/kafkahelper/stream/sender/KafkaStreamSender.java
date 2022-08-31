package com.cmd.kafkahelper.stream.sender;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaOutbound;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

@Log4j2
public class KafkaStreamSender implements StreamSender {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .registerModule(new JavaTimeModule());
    private final KafkaOutbound<String, String> kafkaOutbound;

    protected KafkaStreamSender(final Map<String, Object> producerConfig) {
        if (producerConfig == null) {
            throw new AssertionError("producerConfig cannot be null");
        }
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.kafkaOutbound = KafkaSender.create(SenderOptions.<String, String>create(producerConfig)).createOutbound();
    }

    @Override
    public Mono<Void> send(String topic, Object message) {
        return kafkaOutbound
                .send(Mono.fromCallable(() -> objectMapper.writeValueAsString(message))
                        .doOnNext(body -> log.debug("body : {}",body))
                        .map(stringMessage -> new ProducerRecord<>(topic, stringMessage)))
                .then();

    }
}
