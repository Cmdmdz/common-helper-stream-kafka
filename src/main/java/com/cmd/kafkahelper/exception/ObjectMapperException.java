package com.cmd.kafkahelper.exception;

public class ObjectMapperException extends RuntimeException {

    public ObjectMapperException(String message) {
        super(message);
    }

    public ObjectMapperException(String message, Throwable err) {
        super(message, err);
    }

}
