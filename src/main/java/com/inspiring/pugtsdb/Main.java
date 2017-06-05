package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.pojo.LongMetric;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws SQLException {
        PugTSDB pugTSDB = new PugTSDB("/tmp/pugtsdb", "na", "na");

        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "val1");
        tags.put("tag2", "val2");

        LongMetric metric = new LongMetric("metrica-long", tags, null, "ok".getBytes());

        pugTSDB.upsert(metric);
    }
}
