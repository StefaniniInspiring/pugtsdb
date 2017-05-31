package com.inspiring.pugtsdb.util;

import java.nio.charset.StandardCharsets;

public class Bytes {

    public static long toLong(byte[] bytes) {
        long value = 0;

        for (int i = 0; i < Long.BYTES; i++) {
            value <<= 8;
            value ^= bytes[i] & 0xFF;
        }

        return value;
    }

    public static byte[] fromLong(long value) {
        byte[] b = new byte[Long.BYTES];

        for (int i = 7; i > 0; i--) {
            b[i] = (byte) value;
            value >>>= 8;
        }

        b[0] = (byte) value;

        return b;
    }

    public static double toDouble(byte[] bytes) {
        return Double.longBitsToDouble(toLong(bytes));
    }

    public static byte[] fromDouble(double d) {
        return fromLong(Double.doubleToRawLongBits(d));
    }

    public static boolean toBoolean(byte[] bytes) {
        return bytes[0] != (byte) 0;
    }

    public static byte[] fromBoolean(boolean b) {
        return new byte[]{b ? (byte) -1 : (byte) 0};
    }

    public static String toUtf8String(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] fromUtf8String(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }
}
