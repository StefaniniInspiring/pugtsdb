package com.inspiring.pugtsdb.util;

import java.util.stream.IntStream;

public class Strings {

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
}
