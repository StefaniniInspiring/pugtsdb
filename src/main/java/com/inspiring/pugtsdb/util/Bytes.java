package com.inspiring.pugtsdb.util;

import com.inspiring.pugtsdb.exception.PugConversionException;
import java.nio.charset.StandardCharsets;

public class Bytes {

    public static Long toLong(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        if (bytes.length != Long.BYTES) {
            throw new PugConversionException(bytes, Long.class, Long.BYTES);
        }

        long value = 0;

        for (int i = 0; i < Long.BYTES; i++) {
            value <<= 8;
            value ^= bytes[i] & 0xFF;
        }

        return value;
    }

    public static byte[] fromLong(Long value) {
        if (value == null) {
            return null;
        }

        byte[] b = new byte[Long.BYTES];
        long longValue = value;

        for (int i = 7; i > 0; i--) {
            b[i] = (byte) longValue;
            longValue >>>= 8;
        }

        b[0] = (byte) longValue;

        return b;
    }

    public static Double toDouble(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        return Double.longBitsToDouble(toLong(bytes));
    }

    public static byte[] fromDouble(Double d) {
        if (d == null) {
            return null;
        }

        return fromLong(Double.doubleToRawLongBits(d));
    }

    public static Boolean toBoolean(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        if (bytes.length != 1) {
            throw new PugConversionException(bytes, Boolean.class, 1);
        }

        return bytes[0] != (byte) 0;
    }

    public static byte[] fromBoolean(Boolean b) {
        if (b == null) {
            return null;
        }

        return new byte[]{b ? (byte) -1 : (byte) 0};
    }

    public static String toUtf8String(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] fromUtf8String(String string) {
        if (string == null) {
            return null;
        }

        return string.getBytes(StandardCharsets.UTF_8);
    }
}
