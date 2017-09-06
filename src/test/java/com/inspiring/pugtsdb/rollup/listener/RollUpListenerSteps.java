package com.inspiring.pugtsdb.rollup.listener;

import com.inspiring.pugtsdb.PugTSDB;
import com.inspiring.pugtsdb.PugTSDBOverH2;
import com.inspiring.pugtsdb.metric.DoubleMetric;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.RollUp;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.aggregation.DoubleSumAggregation;
import com.inspiring.pugtsdb.rollup.listen.RollUpEvent;
import com.inspiring.pugtsdb.rollup.listen.RollUpListener;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.inspiring.pugtsdb.util.Temporals.truncate;
import static java.time.ZoneId.systemDefault;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@SuppressWarnings("Duplicates")
public class RollUpListenerSteps {

    private Metric metric;
    private Aggregation aggregation;
    private Retention retention;
    private RollUp rollUp;
    private RollUpListener listener;
    private PugTSDBOverH2 pugTSDB;
    private Repositories repositories;

    private Instant executionInstant;
    private AtomicReference<RollUpEvent> actualEvent = new AtomicReference<>();

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
    public void cleanup() throws Exception {
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
        rollUp = new RollUp(metric.getName(), aggregation, sourceGranularity, targetGranularity, repositories);
    }

    @Given("^a rollup listener$")
    public void aRollupListener() throws Throwable {
        listener = event -> actualEvent.set(event);
    }

    @Given("^a point on \"([^\"]*)\" \"([^\"]*)\" plus (\\d+) \"([^\"]*)\" with a double (\\d+)$")
    public void aPointOnPlusWithADouble(String timestampState,
                                        String timestampUnitString,
                                        long amountToAddValue,
                                        String amountToAddUnit,
                                        Double pointValue) throws Throwable {
        insertPoint(resolveTimestamp(timestampState, timestampUnitString, amountToAddValue, amountToAddUnit), pointValue);
    }

    @When("^the listener is added to rollup$")
    public void theListenerIsAddedToRollup() throws Throwable {
        rollUp.setListener(listener);
    }

    @When("^the rollup executes$")
    public void theRollupExecutes() throws Throwable {
        rollUp.run();
        Thread.sleep(100);
    }

    @Then("^the listener wont be called$")
    public void theListenerWontBeCalled() throws Throwable {
        assertNull(actualEvent.get());
    }

    @Then("^the listener will be called$")
    public void theListenerWillBeCalled() throws Throwable {
        assertNotNull(actualEvent.get());
    }

    @Then("^the event will have a metric name of \"([^\"]*)\"$")
    public void theEventWillHaveAMetricNameOf(String expectedMetricName) throws Throwable {
        assertEquals(expectedMetricName, actualEvent.get().getMetricName());
    }

    @Then("^the event will have a aggregation name of \"([^\"]*)\"$")
    public void theEventWillHaveAAggregationNameOf(String expectedAggregationName) throws Throwable {
        assertEquals(expectedAggregationName, actualEvent.get().getAggregationName());
    }

    @Then("^the event will have a source granularity of (\\d+) \"([^\"]*)\"$")
    public void theEventWillHaveASourceGranularityOf(long granularityValue, String granularityUnit) throws Throwable {
        Granularity expectedGranularity = granularityOf(granularityValue, granularityUnit);
        assertEquals(expectedGranularity, actualEvent.get().getSourceGranularity());
    }

    @Then("^the event will have a target granularity of (\\d+) \"([^\"]*)\"$")
    public void theEventWillHaveATargetGranularityOf(long granularityValue, String granularityUnit) throws Throwable {
        Granularity expectedGranularity = granularityOf(granularityValue, granularityUnit);
        assertEquals(expectedGranularity, actualEvent.get().getTargetGranularity());
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

    private void insertPoint(Timestamp timestamp, Object value) throws Throwable {
        Granularity sourceGranularity = rollUp.getSourceGranularity();
        String sql = sourceGranularity == null
                     ? " INSERT INTO point (\"metric_id\", \"timestamp\", \"value\") VALUES (?, ?, ?) "
                     : " INSERT INTO point_" + sourceGranularity + " (\"metric_id\", \"timestamp\", \"value\", \"aggregation\") VALUES (?, ?, ?, ?) ";

        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, metric.getId());
            statement.setTimestamp(2, timestamp);
            statement.setBytes(3, metric.valueToBytes(value));

            if (sourceGranularity != null) {
                statement.setString(4, aggregation.getName());
            }

            statement.execute();
        }
    }

    private Granularity granularityOf(long granularityValue, String granularityUnit) {
        ChronoUnit unit = ChronoUnit.valueOf(granularityUnit.toUpperCase());
        return unit == ChronoUnit.MILLIS ? null : Granularity.valueOf(granularityValue, unit);
    }
}
