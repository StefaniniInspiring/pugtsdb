package com.inspiring.pugtsdb.selection;

import com.inspiring.pugtsdb.PugTSDBOverH2;
import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.bean.Tag;
import com.inspiring.pugtsdb.metric.DoubleMetric;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Interval;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.Double.parseDouble;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class SelectionSteps {

    private Granularity granularity;
    private PugTSDBOverH2 pugTSDB;
    private Repositories repositories;

    private MetricPoints actualMetricPoints;
    private List<MetricPoints<Object>> actualMetricsPoints;

    @Before
    public void prepare() {
        pugTSDB = new PugTSDBOverH2("/tmp/pug-rollup-test", "test", "test");

        repositories = Stream.of(pugTSDB.getClass().getDeclaredFields())
                .filter(field -> {
                    field.setAccessible(true);
                    return field.isAccessible();
                })
                .filter(field -> field.getType().equals(Repositories.class))
                .map(field -> {
                    try {
                        return (Repositories) field.get(pugTSDB);
                    } catch (IllegalAccessException e) {
                        return null;
                    }
                })
                .findFirst()
                .get();
    }

    @After
    public void cleanup() throws SQLException {
        if (pugTSDB != null) {
            try (Statement statement = pugTSDB.getDataSource().getConnection().createStatement()) {
                statement.execute(" DROP ALL OBJECTS DELETE FILES ");
            } finally {
                pugTSDB.close();
            }
        }
    }

    @Given("^a granularity of (\\d+) \"([^\"]*)\"$")
    public void aGranularityOf(long value, String unitString) throws Throwable {
        if (!ChronoUnit.MILLIS.toString().equalsIgnoreCase(unitString)) {
            granularity = Granularity.valueOf(value, ChronoUnit.valueOf(unitString.toUpperCase()));
        }
    }

    @Given("^the points for metric \"([^\"]*)\":$")
    public void thePointsForMetric(String metricName, List<List<String>> rows) throws Throwable {
        thePointsForMetricWithTag(metricName, null, null, rows);
    }

    @Given("^the points for metric \"([^\"]*)\" with tag \"([^\"]*)\" = \"([^\"]*)\":$")
    public void thePointsForMetricWithTag(String metricName, String tagName, String tagValue, List<List<String>> rows) throws Throwable {
        Map<String, String> tags = tagName != null
                                   ? singletonMap(tagName, tagValue)
                                   : emptyMap();

        Metric<Double> metric = new DoubleMetric(metricName, tags);

        if (repositories.getMetricRepository().notExistsMetric(metric)) {
            repositories.getMetricRepository().insertMetric(metric);
            repositories.getMetricRepository().getConnection().commit();
            repositories.getMetricRepository().getConnection().close();
        }

        if (granularity == null) {
            String sql = "MERGE INTO point (\"metric_id\", \"timestamp\", \"value\") VALUES (?, ?, ?)";

            for (List<String> row : rows) {
                try (Connection connection = pugTSDB.getDataSource().getConnection();
                     PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setInt(1, metric.getId());
                    statement.setTimestamp(2, new Timestamp(parseTime(row.get(0))));
                    statement.setBytes(3, metric.valueToBytes(parseDouble(row.get(1))));
                    statement.execute();
                }
            }
        } else {
            String sql = "MERGE INTO point_" + granularity + " (\"metric_id\", \"timestamp\", \"aggregation\", \"value\") VALUES (?, ?, ?, ?)";

            for (List<String> row : rows) {
                try (Connection connection = pugTSDB.getDataSource().getConnection();
                     PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setInt(1, metric.getId());
                    statement.setTimestamp(2, new Timestamp(parseTime(row.get(0))));
                    statement.setString(3, row.get(1));
                    statement.setBytes(4, metric.valueToBytes(parseDouble(row.get(2))));
                    statement.execute();
                }
            }
        }
    }

    @When("^select points for metric \"([^\"]*)\" ID aggregated as \"([^\"]*)\" between \"([^\"]*)\" and \"([^\"]*)\"$")
    public void selectPointsForMetricIDAggregatedAsBetweenAnd(String metricName, String aggregationName, String fromTimeString, String toTimeString) throws Throwable {
        Interval interval = Interval.until(parseTime(toTimeString)).from(parseTime(fromTimeString));
        Metric<Double> metric = new DoubleMetric(metricName, emptyMap());
        actualMetricPoints = pugTSDB.selectMetricPoints(metric, aggregationName, granularity, interval);
    }

    @When("^select points for metric \"([^\"]*)\" with tag \"([^\"]*)\" = \"([^\"]*)\" aggregated as \"([^\"]*)\" between \"([^\"]*)\" and \"([^\"]*)\"$")
    public void selectPointsForMetricWithTagAggregatedAsBetweenAnd(String metricName,
                                                                   String tagName,
                                                                   String tagValue,
                                                                   String aggregationName,
                                                                   String fromTimeString,
                                                                   String toTimeString) throws Throwable {
        Tag tag = Tag.of(tagName, tagValue);
        Interval interval = Interval.until(parseTime(toTimeString)).from(parseTime(fromTimeString));
        actualMetricsPoints = pugTSDB.selectMetricsPoints(metricName, aggregationName, granularity, interval, tag);
    }

    @When("^select points for metric \"([^\"]*)\" ID between \"([^\"]*)\" and \"([^\"]*)\"$")
    public void selectPointsForMetricIDBetweenAnd(String metricName, String fromTimeString, String toTimeString) throws Throwable {
        Interval interval = Interval.until(parseTime(toTimeString)).from(parseTime(fromTimeString));
        Metric<Double> metric = new DoubleMetric(metricName, emptyMap());
        actualMetricPoints = granularity == null
                             ? pugTSDB.selectMetricPoints(metric, interval)
                             : pugTSDB.selectMetricPoints(metric, granularity, interval);
    }

    @When("^select points for metric \"([^\"]*)\" with tag \"([^\"]*)\" = \"([^\"]*)\" between \"([^\"]*)\" and \"([^\"]*)\"$")
    public void selectPointsForMetricWithTagBetweenAnd(String metricName, String tagName, String tagValue, String fromTimeString, String toTimeString) throws Throwable {
        Tag tag = Tag.of(tagName, tagValue);
        Interval interval = Interval.until(parseTime(toTimeString)).from(parseTime(fromTimeString));
        actualMetricsPoints = granularity == null
                              ? pugTSDB.selectMetricsPoints(metricName, interval, tag)
                              : pugTSDB.selectMetricsPoints(metricName, granularity, interval, tag);
    }

    @Then("^the select returns no metric points$")
    public void theSelectReturnsNoMetricPoints() throws Throwable {
        assertNull(actualMetricPoints);
    }

    @Then("^the select returns no metrics points$")
    public void theSelectReturnsNoMetricsPoints() throws Throwable {
        assertNotNull(actualMetricsPoints);
        assertTrue(actualMetricsPoints.isEmpty());
    }

    @Then("^the select returns a metric points for \"([^\"]*)\"$")
    public void theSelectReturnsAMetricPointsFor(String expectedMetricName) throws Throwable {
        assertNotNull(actualMetricPoints);
        assertNotNull(actualMetricPoints.getMetric());
        assertEquals(expectedMetricName, actualMetricPoints.getMetric().getName());
    }

    @Then("^the select returns (\\d+) metric points for \"([^\"]*)\" with tag \"([^\"]*)\" = \"([^\"]*)\"$")
    public void theSelectReturnsMetricPointsForWithTag(long expectedMetricsPointsQty, String metricName, String tagName, String tagValue) throws Throwable {
        assertNotNull(actualMetricsPoints);

        long actualMetricsPointsQty = actualMetricsPoints.stream()
                .filter(metricPoints -> metricPoints.getMetric().getName().equals(metricName))
                .filter(metricPoints -> metricPoints.getMetric().getTags().equals(singletonMap(tagName, tagValue)))
                .count();

        assertEquals(expectedMetricsPointsQty, actualMetricsPointsQty);
    }

    @Then("^the metric points contains (\\d+) raw points$")
    public void theMetricPointsContainsRawPoints(int expectedPoints) throws Throwable {
        theMetricPointsContainsPointsAggregatedAs(expectedPoints, null);
    }

    @Then("^the metric points contains (\\d+) points aggregated as \"([^\"]*)\"$")
    public void theMetricPointsContainsPointsAggregatedAs(int expectedPoints, String aggregationName) throws Throwable {
        Map<String, Map<Long, Object>> points = actualMetricPoints.getPoints();
        assertTrue(points.containsKey(aggregationName));
        assertEquals(expectedPoints, points.get(aggregationName).size());
    }

    @Then("^the metric \"([^\"]*)\" with tag \"([^\"]*)\" = \"([^\"]*)\" contains (\\d+) raw points$")
    public void theMetricWithTagContainsRawPoints(String metricName, String tagName, String tagValue, int expectedPoints) throws Throwable {
        theMetricWithTagContainsPointsAggregatedAs(metricName, tagName, tagValue, expectedPoints, null);
    }

    @Then("^the metric \"([^\"]*)\" with tag \"([^\"]*)\" = \"([^\"]*)\" contains (\\d+) points aggregated as \"([^\"]*)\"$")
    public void theMetricWithTagContainsPointsAggregatedAs(String metricName,
                                                           String tagName,
                                                           String tagValue,
                                                           int expectedPoints,
                                                           String aggregationName) throws Throwable {
        int actualPoints = actualMetricsPoints.stream()
                .filter(metricPoints -> metricPoints.getMetric().getName().equals(metricName))
                .filter(metricPoints -> metricPoints.getMetric().getTags().equals(singletonMap(tagName, tagValue)))
                .map(metricPoints -> metricPoints.getPoints().getOrDefault(aggregationName, emptyMap()).size())
                .findFirst()
                .orElse(0);

        assertEquals(expectedPoints, actualPoints);
    }

    @Then("^the metric points contains a raw point on \"([^\"]*)\" with value (\\d+)$")
    public void theMetricPointsContainsARawPointOnWithValue(String timestampString, Double expectedValue) throws Throwable {
        theMetricPointsContainsAPointAggregatedAsOnWithValue(null, timestampString, expectedValue);
    }

    @Then("^the metric points contains a point aggregated as \"([^\"]*)\" on \"([^\"]*)\" with value (\\d+)$")
    public void theMetricPointsContainsAPointAggregatedAsOnWithValue(String aggregationName, String timestampString, Double expectedValue) throws Throwable {
        Map<String, Map<Long, Object>> points = actualMetricPoints.getPoints();
        assertTrue(points.containsKey(aggregationName));
        assertEquals(expectedValue, points.get(aggregationName).get(parseTime(timestampString)));
    }

    @Then("^the metric \"([^\"]*)\" with tag \"([^\"]*)\" = \"([^\"]*)\" contains a raw point on \"([^\"]*)\" with value (\\d+)$")
    public void theMetricWithTagContainsARawPointOnWithValue(String metricName,
                                                             String tagName,
                                                             String tagValue,
                                                             String timestampString,
                                                             Double expectedValue) throws Throwable {
        theMetricWithTagContainsAPointAggregatedAsOnWithValue(metricName, tagName, tagValue, null, timestampString, expectedValue);
    }

    @Then("^the metric \"([^\"]*)\" with tag \"([^\"]*)\" = \"([^\"]*)\" contains a point aggregated as \"([^\"]*)\" on \"([^\"]*)\" with value (\\d+)$")
    public void theMetricWithTagContainsAPointAggregatedAsOnWithValue(String metricName,
                                                                      String tagName,
                                                                      String tagValue,
                                                                      String aggregationName,
                                                                      String timestampString,
                                                                      Double expectedValue) throws Throwable {
        Map<Long, Object> points = actualMetricsPoints.stream()
                .filter(metricPoints -> metricPoints.getMetric().getName().equals(metricName))
                .filter(metricPoints -> metricPoints.getMetric().getTags().equals(singletonMap(tagName, tagValue)))
                .map(metricPoints -> metricPoints.getPoints().get(aggregationName))
                .findFirst()
                .orElse(null);

        assertNotNull(points);

        Object actualValue = points.get(parseTime(timestampString));

        assertEquals(expectedValue, actualValue);
    }

    private long parseTime(String string) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").parse(string).getTime();
    }
}
