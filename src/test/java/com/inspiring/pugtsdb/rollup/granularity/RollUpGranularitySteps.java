package com.inspiring.pugtsdb.rollup.granularity;

import com.inspiring.pugtsdb.PugTSDBOverH2;
import com.inspiring.pugtsdb.metric.DoubleMetric;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.RollUp;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.aggregation.DoubleSumAggregation;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.inspiring.pugtsdb.util.Temporals.truncate;
import static java.time.ZoneId.systemDefault;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class RollUpGranularitySteps {

    private Metric metric;
    private Aggregation aggregation;
    private Retention retention;
    private PugTSDBOverH2 pugTSDB;
    private Repositories repositories;
    private Instant executionInstant;
    private List<RollUp> rollUps = new ArrayList<>();

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

    @Given("^a double metric named \"([^\"]*)\"$")
    public void aDoubleMetricNamed(String name) throws Throwable {
        metric = new DoubleMetric(name, emptyMap());
        repositories.getMetricRepository().insertMetric(metric);
        repositories.getMetricRepository().getConnection().commit();
        repositories.getMetricRepository().getConnection().close();
    }

    @Given("^a SUM aggregation of double values$")
    public void aSUMAggregationOfDoubleValues() throws Throwable {
        aggregation = new DoubleSumAggregation();
    }

    @Given("^a retention of (\\d+) \"([^\"]*)\"$")
    public void aRetentionOf(long value, String unitString) throws Throwable {
        retention = Retention.of(value, ChronoUnit.valueOf(unitString.toUpperCase()));
    }

    @Given("^a rollup from (\\d+) \"([^\"]*)\" to (\\d+) \"([^\"]*)\"$")
    public void aRollupFromTo(long sourceGranularityValue, String sourceGranularityUnit, long targetGranularityValue, String targetGranularityUnit) throws Throwable {
        Granularity sourceGranularity = granularityOf(sourceGranularityValue, sourceGranularityUnit);
        Granularity targetGranularity = granularityOf(targetGranularityValue, targetGranularityUnit);
        rollUps.add(new RollUp(metric.getName(), aggregation, sourceGranularity, targetGranularity, retention, repositories));
    }

    @Given("^a point on \"([^\"]*)\" \"([^\"]*)\" with a double (\\d+)$")
    public void aPointOnWithADouble(String timestampState, String timestampUnitString, Double value) throws Throwable {
        String sql = " INSERT INTO point (\"metric_id\", \"timestamp\", \"value\") VALUES (?, ?, ?) ";

        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, metric.getId());
            statement.setTimestamp(2, resolveTimestamp(timestampState, timestampUnitString));
            statement.setBytes(3, metric.valueToBytes(value));
            statement.execute();
        }
    }

    @When("^the rollups executes$")
    public void theRollupsExecutes() throws Throwable {
        for (RollUp rollUp : rollUps) {
            rollUp.run();
        }
    }

    @Then("^no points are rolled up by (\\d+) \"([^\"]*)\"$")
    public void noPointsAreRolledUpBy(long granularityValue, String granularityUnit) throws Throwable {
        String sql = ""
                + " SELECT \"value\"           "
                + " FROM   point_" + granularityOf(granularityValue, granularityUnit)
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

    @And("^a point on \"([^\"]*)\" \"([^\"]*)\" will be rolled up by (\\d+) \"([^\"]*)\" with a double (\\d+)$")
    public void aPointOnWillBeRolledUpByWithADouble(String timestampState,
                                                    String timestampUnit,
                                                    long granularityValue,
                                                    String granularityUnit,
                                                    Double expectedValue) throws Throwable {
        String sql = ""
                + " SELECT \"value\"          "
                + " FROM   point_" + granularityOf(granularityValue, granularityUnit)
                + " WHERE  \"metric_id\" = ?   "
                + " AND    \"timestamp\" = ?   "
                + " AND    \"aggregation\" = ? ";

        Timestamp timestamp = resolveTimestamp(timestampState, timestampUnit);

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

    private Granularity granularityOf(long granularityValue, String granularityUnit) {
        ChronoUnit unit = ChronoUnit.valueOf(granularityUnit.toUpperCase());
        return unit == ChronoUnit.MILLIS ? null : Granularity.valueOf(granularityValue, unit);
    }

    private Timestamp resolveTimestamp(String timestampState, String timestampUnitString) {
        ChronoUnit timestampUnit = ChronoUnit.valueOf(timestampUnitString.toUpperCase());
        ZonedDateTime timestamp = truncate(executionInstant.atZone(systemDefault()), timestampUnit);

        if (timestampState.equalsIgnoreCase("past")) {
            timestamp = timestamp.minus(1, timestampUnit);
        } else if (timestampState.equalsIgnoreCase("future")) {
            timestamp = timestamp.plus(1, timestampUnit);
        }

        return new Timestamp(timestamp.toInstant().toEpochMilli());
    }
}
