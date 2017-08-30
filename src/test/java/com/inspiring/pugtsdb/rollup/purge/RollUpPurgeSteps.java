package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.PugTSDB;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class RollUpPurgeSteps {

    private Metric metric;
    private Aggregation aggregation;
    private Retention retention;
    private RollUp rollUp;
    private PugTSDBOverH2 pugTSDB;
    private Repositories repositories;
    private Instant executionInstant;

    @Before
    public void prepare() {
        pugTSDB = new PugTSDBOverH2("/tmp/pug-rollup-test", "test", "test");

        repositories = Stream.of(PugTSDB.class.getDeclaredFields())
                .filter(field -> {
                    field.setAccessible(true);
                    return field.isAccessible();
                })
                .filter(field -> field.getType().isAssignableFrom(Repositories.class))
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
        rollUp = new RollUp(metric.getName(), aggregation, sourceGranularity, targetGranularity, retention, repositories);
    }

    @Given("^a rolled up point on \"([^\"]*)\" (\\d+) \"([^\"]*)\" with a double (\\d+)$")
    public void aRolledUpPointOnWithADouble(String timestampState,
                                            long timestampDiff,
                                            String timestampUnit,
                                            Double value) throws Throwable {
        String sql = ""
                + " INSERT INTO point_" + rollUp.getTargetGranularity()
                + " (\"metric_id\", \"timestamp\", \"aggregation\", \"value\") "
                + " VALUES (?, ?, ?, ?) ";

        Timestamp timestamp = resolveTimestamp(timestampState, timestampDiff, timestampUnit);

        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, metric.getId());
            statement.setTimestamp(2, timestamp);
            statement.setString(3, aggregation.getName());
            statement.setBytes(4, metric.valueToBytes(value));
            statement.execute();
        }
    }

    @When("^the rollup executes$")
    public void theRollupExecutes() throws Throwable {
        rollUp.run();
    }

    @Then("^the rolled up point on \"([^\"]*)\" (\\d+) \"([^\"]*)\" wont be purged$")
    public void theRolledUpPointOnWontBePurged(String timestampState,
                                               long timestampDiff,
                                               String timestampUnit) throws Throwable {
        String sql = ""
                + " SELECT * FROM point_" + rollUp.getTargetGranularity()
                + " WHERE \"metric_id\" = ?   "
                + " AND   \"timestamp\" = ?   "
                + " AND   \"aggregation\" = ? ";

        Timestamp timestamp = resolveTimestamp(timestampState, timestampDiff, timestampUnit);

        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, metric.getId());
            statement.setTimestamp(2, timestamp);
            statement.setString(3, aggregation.getName());
            ResultSet resultSet = statement.executeQuery();

            assertTrue(resultSet.next());
        }
    }

    @Then("^the rolled up point on \"([^\"]*)\" (\\d+) \"([^\"]*)\" will be purged$")
    public void theRolledUpPointOnWillBePurged(String timestampState,
                                               long timestampDiff,
                                               String timestampUnit) throws Throwable {
        String sql = ""
                + " SELECT * FROM point_" + rollUp.getTargetGranularity()
                + " WHERE \"metric_id\" = ?   "
                + " AND   \"timestamp\" = ?   "
                + " AND   \"aggregation\" = ? ";

        Timestamp timestamp = resolveTimestamp(timestampState, timestampDiff, timestampUnit);

        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, metric.getId());
            statement.setTimestamp(2, timestamp);
            statement.setString(3, aggregation.getName());
            ResultSet resultSet = statement.executeQuery();

            assertFalse(resultSet.next());
        }
    }

    private Granularity granularityOf(long granularityValue, String granularityUnit) {
        ChronoUnit unit = ChronoUnit.valueOf(granularityUnit.toUpperCase());
        return unit == ChronoUnit.MILLIS ? null : Granularity.valueOf(granularityValue, unit);
    }

    private Timestamp resolveTimestamp(String state, long diff, String unitString) {
        ChronoUnit timestampUnit = ChronoUnit.valueOf(unitString.toUpperCase());
        ZonedDateTime timestamp = truncate(executionInstant.atZone(systemDefault()), timestampUnit);

        if (state.equalsIgnoreCase("past")) {
            timestamp = timestamp.minus(diff, timestampUnit);
        } else if (state.equalsIgnoreCase("future")) {
            timestamp = timestamp.plus(diff, timestampUnit);
        }

        return new Timestamp(timestamp.toInstant().toEpochMilli());
    }
}

