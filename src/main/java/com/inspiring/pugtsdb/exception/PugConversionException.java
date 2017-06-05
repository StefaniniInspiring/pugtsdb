package com.inspiring.pugtsdb.exception;

import java.text.MessageFormat;
import java.util.Arrays;

public class PugConversionException extends PugException {

    public PugConversionException(byte[] fromBytes, Class<?> toClass, int expectedLength) {
        super(MessageFormat.format("Cannot convert bytes {0} to {1}: Expected a length of {2}, got {3}",
                                   Arrays.toString(fromBytes),
                                   toClass.getSimpleName(),
                                   expectedLength,
                                   fromBytes.length));
    }
}
