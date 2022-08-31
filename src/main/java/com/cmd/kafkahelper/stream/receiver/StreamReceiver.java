package com.cmd.kafkahelper.stream.receiver;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;

import java.util.function.Function;

public interface StreamReceiver<T> {

    Disposable receive(Function<? super T, ? extends Publisher<?>> handler);
}
