package com.inspiring.pugtsdb.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.inspiring.pugtsdb.util.Strings.isBlank;

public class GlobPattern {

    private static final Pattern GLOB_PATTERN = Pattern.compile("\\?|\\*|\\{((?:\\{[^/]+?\\}|[^/{}]|\\\\[{}])+?)\\}");

    private static final String DEFAULT_VARIABLE_PATTERN = "(.*)";

    public static Pattern compile(String pattern) {
        StringBuilder patternBuilder = new StringBuilder();
        Matcher matcher = GLOB_PATTERN.matcher(pattern);
        int end = 0;

        while (matcher.find()) {
            patternBuilder.append(quote(pattern, end, matcher.start()));
            String match = matcher.group();

            if ("?".equals(match)) {
                patternBuilder.append('.');
            } else if ("*".equals(match)) {
                patternBuilder.append(".*");
            } else if (match.startsWith("{") && match.endsWith("}")) {
                int colonIdx = match.indexOf(':');

                if (colonIdx == -1) {
                    patternBuilder.append(DEFAULT_VARIABLE_PATTERN);
                } else {
                    String variablePattern = match.substring(colonIdx + 1, match.length() - 1);
                    patternBuilder.append('(');
                    patternBuilder.append(variablePattern);
                    patternBuilder.append(')');
                }
            }

            end = matcher.end();
        }

        patternBuilder.append(quote(pattern, end, pattern.length()));

        return Pattern.compile(patternBuilder.toString());
    }

    public static boolean isGlob(String pattern) {
        return isBlank(pattern) || pattern.contains("*") || pattern.contains("?");
    }

    private static String quote(String s, int start, int end) {
        if (start == end) {
            return "";
        }

        return Pattern.quote(s.substring(start, end));
    }
}
