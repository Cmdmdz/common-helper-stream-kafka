package com.cmd.kafkahelper.stream.sender;

import reactor.core.publisher.Mono;

public interface StreamSender {

    Mono<Void> send(String topic, Object message);
}
