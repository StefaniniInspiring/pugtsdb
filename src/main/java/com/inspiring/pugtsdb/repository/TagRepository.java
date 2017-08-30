package com.inspiring.pugtsdb.repository;

import java.util.Map;

public interface TagRepository extends Repository {

    Map<String, String> selectTagsByMetricId(String metricId);

    void upsertTags(Map<String, String> tags);
}
