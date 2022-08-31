package com.cmd.kafkahelper.stream.receiver;

import com.cmd.kafkahelper.exception.ObjectMapperException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

@Slf4j
public class KafkaStreamReceiver<T> implements StreamReceiver<T> {
    private final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final KafkaReceiver<String, String> kafkaReceiver;
    private final Class<T> valueTypeClass;
    private final String topic;

    protected KafkaStreamReceiver(final Map<String, Object> consumerConfig, final String topic, final Class<T> valueTypeClass) {

        if (consumerConfig == null) {
            throw new AssertionError("consumerConfig cannot be null");
        }

        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.topic = topic;
        this.valueTypeClass = valueTypeClass;
        this.kafkaReceiver = KafkaReceiver.create(ReceiverOptions.<String, String>create(consumerConfig).subscription(Collections.singletonList(topic)));
    }



    @Override
    public Disposable receive(Function<? super T, ? extends Publisher<?>> handler) {
        return kafkaReceiver.receive()
                .map(receiveRecord -> {
                    try {
                        final String body = receiveRecord.value();
                        return objectMapper.readValue(body, valueTypeClass);
                    } catch (JsonProcessingException exception) {
                        throw new ObjectMapperException("Object Mapper cannot read value", exception);
                    }
                })
                .onErrorContinue(((throwable, o) -> log.error("Failed to read JSON body from stream: {}", o, throwable)))
                .flatMap(handler)
                .onErrorContinue(((throwable, o) -> log.error("Failed to execute the stream handler: {}", o, throwable)))
                .subscribe();

    }
}
