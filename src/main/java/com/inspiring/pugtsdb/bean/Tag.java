package com.inspiring.pugtsdb.bean;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Comparator.naturalOrder;
import static java.util.function.BinaryOperator.maxBy;

public class Tag {

    private final String name;
    private final String value;

    public Tag(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Tag{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    public static Tag of(String name, String value) {
        return new Tag(name, value);
    }

    public static Map<String, String> toMap(Tag... tags) {
        if (tags == null) {
            return emptyMap();
        }

        return Stream.of(tags)
                .collect(Collectors.toMap(Tag::getName, Tag::getValue, maxBy(naturalOrder())));
    }
}
