package com.inspiring.pugtsdb.rollup.aggregation;

import com.inspiring.pugtsdb.PugTSDB;
import com.inspiring.pugtsdb.metric.BooleanMetric;
import com.inspiring.pugtsdb.metric.DoubleMetric;
import com.inspiring.pugtsdb.metric.LongMetric;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.metric.StringMetric;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.RollUp;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import static com.inspiring.pugtsdb.util.Temporals.truncate;
import static java.time.ZoneId.systemDefault;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class RollUpAggregationSteps {

    private Metric metric;
    private Aggregation aggregation;
    private Granularity sourceGranularity;
    private Granularity targetGranularity;
    private Retention retention;
    private RollUp rollUp;
    private PugTSDB pugTSDB;
    private Repositories repositories;

    private Instant executionInstant;

    @Before
    public void prepare() {
        pugTSDB = new PugTSDB("/tmp/pug-rollup-test", "test", "test");

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

        executionInstant = Instant.now();
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

    @Given("^a boolean metric named \"([^\"]*)\"$")
    public void aBooleanMetricNamed(String name) throws Throwable {
        metric = new BooleanMetric(name, emptyMap());
        repositories.getMetricRepository().insertMetric(metric);
        repositories.getMetricRepository().getConnection().commit();
        repositories.getMetricRepository().getConnection().close();
    }

    @Given("^a double metric named \"([^\"]*)\"$")
    public void aDoubleMetricNamed(String name) throws Throwable {
        metric = new DoubleMetric(name, emptyMap());
        repositories.getMetricRepository().insertMetric(metric);
        repositories.getMetricRepository().getConnection().commit();
        repositories.getMetricRepository().getConnection().close();
    }

    @Given("^a long metric named \"([^\"]*)\"$")
    public void aLongMetricNamed(String name) throws Throwable {
        metric = new LongMetric(name, emptyMap());
        repositories.getMetricRepository().insertMetric(metric);
        repositories.getMetricRepository().getConnection().commit();
        repositories.getMetricRepository().getConnection().close();
    }

    @Given("^a string metric named \"([^\"]*)\"$")
    public void aStringMetricNamed(String name) throws Throwable {
        metric = new StringMetric(name, emptyMap());
        repositories.getMetricRepository().insertMetric(metric);
        repositories.getMetricRepository().getConnection().commit();
        repositories.getMetricRepository().getConnection().close();
    }

    @Given("^an AND aggregation of boolean values$")
    public void anANDAggregationOfBooleanValues() throws Throwable {
        aggregation = new BooleanAndAggregation();
    }

    @Given("^an OR aggregation of boolean values$")
    public void anORAggregationOfBooleanValues() throws Throwable {
        aggregation = new BooleanOrAggregation();
    }

    @Given("^a SUM aggregation of double values$")
    public void aSUMAggregationOfDoubleValues() throws Throwable {
        aggregation = new DoubleSumAggregation();
    }

    @Given("^a MIN aggregation of double values$")
    public void aMINAggregationOfDoubleValues() throws Throwable {
        aggregation = new DoubleMinAggregation();
    }

    @Given("^a MAX aggregation of double values$")
    public void aMAXAggregationOfDoubleValues() throws Throwable {
        aggregation = new DoubleMaxAggregation();
    }

    @Given("^an AVG aggregation of double values$")
    public void anAVGAggregationOfDoubleValues() throws Throwable {
        aggregation = new DoubleAvgAggregation();
    }

    @Given("^a SUM aggregation of long values$")
    public void aSUMAggregationOfLongValues() throws Throwable {
        aggregation = new LongSumAggregation();
    }

    @Given("^a MIN aggregation of long values$")
    public void aMINAggregationOfLongValues() throws Throwable {
        aggregation = new LongMinAggregation();
    }

    @Given("^a MAX aggregation of long values$")
    public void aMAXAggregationOfLongValues() throws Throwable {
        aggregation = new LongMaxAggregation();
    }

    @Given("^an AVG aggregation of long values$")
    public void anAVGAggregationOfLongValues() throws Throwable {
        aggregation = new LongAvgAggregation();
    }

    @Given("^a SUM aggregation of string values$")
    public void aSUMAggregationOfStringValues() throws Throwable {
        aggregation = new StringSumAggregation();
    }

    @Given("^a MIN aggregation of string values$")
    public void aMINAggregationOfStringValues() throws Throwable {
        aggregation = new StringMinAggregation();
    }

    @Given("^a MAX aggregation of string values$")
    public void aMAXAggregationOfStringValues() throws Throwable {
        aggregation = new StringMaxAggregation();
    }

    @Given("^a source granularity of (\\d+) \"([^\"]*)\"$")
    public void aSourceGranularityOf(long value, String unitString) throws Throwable {
        if (!ChronoUnit.MILLIS.toString().equalsIgnoreCase(unitString)) {
            sourceGranularity = Granularity.valueOf(value, ChronoUnit.valueOf(unitString.toUpperCase()));
        }
    }

    @Given("^a target granularity of (\\d+) \"([^\"]*)\"$")
    public void aTargetGranularityOf(long value, String unitString) throws Throwable {
        if (!ChronoUnit.MILLIS.toString().equalsIgnoreCase(unitString)) {
            targetGranularity = Granularity.valueOf(value, ChronoUnit.valueOf(unitString.toUpperCase()));
        }
    }

    @Given("^a retention of (\\d+) \"([^\"]*)\"$")
    public void aRetentionOf(long value, String unitString) throws Throwable {
        retention = Retention.of(value, ChronoUnit.valueOf(unitString.toUpperCase()));
    }

    @Given("^a rollup instance$")
    public void aRollupInstance() throws Throwable {
        rollUp = new RollUp(metric.getName(), aggregation, sourceGranularity, targetGranularity, retention, repositories);
    }

    @Given("^a point on \"([^\"]*)\" \"([^\"]*)\" plus (\\d+) \"([^\"]*)\" with a boolean \"([^\"]*)\"$")
    public void aPointOnPlusWithValue(String timestampState,
                                      String timestampUnitString,
                                      long amountToAddValue,
                                      String amountToAddUnit,
                                      Boolean pointValue) throws Throwable {
        insertPoint(resolveTimestamp(timestampState, timestampUnitString, amountToAddValue, amountToAddUnit), pointValue);
    }

    @Given("^a point on \"([^\"]*)\" \"([^\"]*)\" plus (\\d+) \"([^\"]*)\" with a double (\\d+)$")
    public void aPointOnPlusWithADouble(String timestampState,
                                        String timestampUnitString,
                                        long amountToAddValue,
                                        String amountToAddUnit,
                                        Double pointValue) throws Throwable {
        insertPoint(resolveTimestamp(timestampState, timestampUnitString, amountToAddValue, amountToAddUnit), pointValue);
    }

    @Given("^a point on \"([^\"]*)\" \"([^\"]*)\" plus (\\d+) \"([^\"]*)\" with a long (\\d+)$")
    public void aPointOnPlusWithALong(String timestampState,
                                      String timestampUnitString,
                                      long amountToAddValue,
                                      String amountToAddUnit,
                                      Long pointValue) throws Throwable {
        insertPoint(resolveTimestamp(timestampState, timestampUnitString, amountToAddValue, amountToAddUnit), pointValue);
    }

    @Given("^a point on \"([^\"]*)\" \"([^\"]*)\" plus (\\d+) \"([^\"]*)\" with a string \"([^\"]*)\"$")
    public void aPointOnPlusWithAString(String timestampState,
                                        String timestampUnitString,
                                        long amountToAddValue,
                                        String amountToAddUnit,
                                        String pointValue) throws Throwable {
        insertPoint(resolveTimestamp(timestampState, timestampUnitString, amountToAddValue, amountToAddUnit), pointValue);
    }

    @Given("^a point on \"([^\"]*)\" \"([^\"]*)\" plus (\\d+) \"([^\"]*)\" with a null value$")
    public void aPointOnPlusWithANullValue(String timestampState, String timestampUnitString, long amountToAddValue, String amountToAddUnit) throws Throwable {
        insertPoint(resolveTimestamp(timestampState, timestampUnitString, amountToAddValue, amountToAddUnit), null);
    }

    private void insertPoint(Timestamp timestamp, Object value) throws Throwable {
        String sql = sourceGranularity == null
                     ? " INSERT INTO point (\"metric_id\", \"timestamp\", \"value\") VALUES (?, ?, ?) "
                     : " INSERT INTO point_" + sourceGranularity + " (\"metric_id\", \"timestamp\", \"value\", \"aggregation\") VALUES (?, ?, ?, ?) ";

        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, metric.getId());
            statement.setTimestamp(2, timestamp);
            statement.setBytes(3, metric.valueToBytes(value));

            if (sourceGranularity != null) {
                statement.setString(4, aggregation.getName());
            }

            statement.execute();
        }
    }

    @When("^the rollup executes$")
    public void theRollupExecutes() throws Throwable {
        rollUp.run();
    }

    @Then("^no points are rolled up$")
    public void noPointsAreRolledUp() throws Throwable {
        String sql = ""
                + " SELECT \"value\"           "
                + " FROM   point_" + targetGranularity
                + " WHERE  \"metric_id\" = ?   "
                + " AND    \"aggregation\" = ? ";

        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, metric.getId());
            statement.setString(2, aggregation.getName());
            ResultSet resultSet = statement.executeQuery();

            assertFalse(resultSet.next());
        }
    }

    @Then("^a point on \"([^\"]*)\" \"([^\"]*)\" will be rolled up with a boolean \"([^\"]*)\"$")
    public void aPointOnWillBeRolledUpWithABoolean(String timestampState, String timestampUnitString, Boolean expectedValue) throws Throwable {
        assertRollUp(timestampState, timestampUnitString, expectedValue);
    }

    @Then("^a point on \"([^\"]*)\" \"([^\"]*)\" will be rolled up with a double (\\d+)$")
    public void aPointOnWillBeRolledUpWithADouble(String timestampState, String timestampUnitString, Double expectedValue) throws Throwable {
        assertRollUp(timestampState, timestampUnitString, expectedValue);
    }

    @Then("^a point on \"([^\"]*)\" \"([^\"]*)\" will be rolled up with a long (\\d+)$")
    public void aPointOnWillBeRolledUpWithALong(String timestampState, String timestampUnitString, Long expectedValue) throws Throwable {
        assertRollUp(timestampState, timestampUnitString, expectedValue);
    }

    @Then("^a point on \"([^\"]*)\" \"([^\"]*)\" will be rolled up with a string \"([^\"]*)\"$")
    public void aPointOnWillBeRolledUpWithAString(String timestampState, String timestampUnitString, String expectedValue) throws Throwable {
        assertRollUp(timestampState, timestampUnitString, expectedValue);
    }

    @Then("^a point on \"([^\"]*)\" \"([^\"]*)\" will be rolled up with null value$")
    public void aPointOnWillBeRolledUpWithNullValue(String timestampState, String timestampUnitString) throws Throwable {
        assertRollUp(timestampState, timestampUnitString, null);
    }

    private void assertRollUp(String timestampState, String timestampUnitString, Object expectedValue) throws Throwable {
        String sql = ""
                + " SELECT \"value\"           "
                + " FROM   point_" + targetGranularity
                + " WHERE  \"metric_id\" = ?   "
                + " AND    \"timestamp\" = ?   "
                + " AND    \"aggregation\" = ? ";

        Timestamp timestamp = resolveTimestamp(timestampState, timestampUnitString, 0, null);

        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, metric.getId());
            statement.setTimestamp(2, timestamp);
            statement.setString(3, aggregation.getName());
            ResultSet resultSet = statement.executeQuery();

            assertTrue(resultSet.next());

            byte[] actualBytes = resultSet.getBytes(1);
            Object actualValue = metric.valueFromBytes(actualBytes);

            if (expectedValue == null) {
                assertNull(actualValue);
            } else {
                assertEquals(expectedValue, actualValue);
            }
        }
    }

    private Timestamp resolveTimestamp(String timestampState, String timestampUnitString, long amountToAddValue, String amountToAddUnit) {
        ChronoUnit timestampUnit = ChronoUnit.valueOf(timestampUnitString.toUpperCase());
        ZonedDateTime timestamp = truncate(executionInstant.atZone(systemDefault()), timestampUnit);

        if (timestampState.equalsIgnoreCase("past")) {
            timestamp = timestamp.minus(1, timestampUnit);
        } else if (timestampState.equalsIgnoreCase("future")) {
            timestamp = timestamp.plus(1, timestampUnit);
        }

        if (amountToAddValue != 0 && amountToAddUnit != null) {
            timestamp = timestamp.plus(amountToAddValue, ChronoUnit.valueOf(amountToAddUnit.toUpperCase()));
        }

        return new Timestamp(timestamp.toInstant().toEpochMilli());
    }
}
