package com.inspiring.pugtsdb.exception;

public class PugException extends RuntimeException {

    public PugException(String message, Throwable cause) {
        super(message, cause);
    }

    public PugException(String message) {
        super(message);
    }
}
