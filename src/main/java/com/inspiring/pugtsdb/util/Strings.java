package com.inspiring.pugtsdb.util;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;

public class Strings {

    private static final Map<Integer, MessageFormat> numberFormatByLength = new ConcurrentHashMap<>();

    public static boolean isNotBlank(final CharSequence cs) {
        return !isBlank(cs);
    }

    public static boolean isBlank(final CharSequence cs) {
        int len;

        if (cs == null || (len = cs.length()) == 0) {
            return true;
        }

        return IntStream.range(0, len).allMatch(i -> Character.isWhitespace(cs.charAt(i)));
    }

    public static String format(double number, int length) {
        if (length < 1) {
            return Objects.toString(number);
        }

        return numberFormatByLength.computeIfAbsent(number < 0 ? length - 1 : length,
                                                    len -> new MessageFormat("{0,number," + repeat("0", len) + "}")).format(new Object[]{number});
    }

    public static String repeat(String s, int n) {
        return IntStream.range(0, n).mapToObj(i -> s).collect(joining());
    }
}
