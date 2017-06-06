package com.inspiring.pugtsdb.upsertion;

import com.inspiring.pugtsdb.PugTSDB;
import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import com.inspiring.pugtsdb.metric.BooleanMetric;
import com.inspiring.pugtsdb.metric.DoubleMetric;
import com.inspiring.pugtsdb.metric.LongMetric;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.metric.StringMetric;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class UpsertionSteps {

    private String metricType;
    private String metricName;
    private Map<String, String> metricTags;
    private Long metricTimestamp;
    private String metricValue;

    private Exception actualException;

    private Metric<?> metric;
    private String storage = "/tmp/pugtest";
    private PugTSDB pugTSDB;

    @Before
    public void startup() {
        pugTSDB = new PugTSDB(storage, "test", "test");
    }

    @After
    public void cleanup() {
        if (pugTSDB != null) {
            pugTSDB.close();
        }

        if (storage != null) {
            Path storagePath = Paths.get(storage);
            File storageDir = storagePath.getParent().toFile();
            String filename = storagePath.getFileName().toString();
            File[] files = storageDir.listFiles((dir, name) -> name.matches(filename + "\\..*"));

            for (File file : files) {
                file.delete();
            }
        }
    }

    @Given("^the type \"([^\"]*)\"$")
    public void theType(String type) throws Throwable {
        metricType = type;
    }

    @Given("^the name \"([^\"]*)\"$")
    public void theName(String name) throws Throwable {
        metricName = name;
    }

    @Given("^the tags:$")
    public void theTags(Map<String, String> tags) throws Throwable {
        metricTags = tags;
    }

    @Given("^the timestamp (\\d+)$")
    public void theTimestamp(long timestamp) throws Throwable {
        metricTimestamp = timestamp;
    }

    @Given("^the value \"([^\"]*)\"$")
    public void theValue(String value) throws Throwable {
        metricValue = value;
    }

    @When("^the metric is created$")
    public void theMetricIsCreated() throws Throwable {
        try {
            switch (metricType) {
                case "Boolean":
                    metric = new BooleanMetric(metricName, metricTags, metricTimestamp, metricValue == null ? null : Boolean.parseBoolean(metricValue));
                    break;
                case "Double":
                    metric = new DoubleMetric(metricName, metricTags, metricTimestamp, metricValue == null ? null : Double.parseDouble(metricValue));
                    break;
                case "Long":
                    metric = new LongMetric(metricName, metricTags, metricTimestamp, metricValue == null ? null : Long.parseLong(metricValue));
                    break;
                default:
                    metric = new StringMetric(metricName, metricTags, metricTimestamp, metricValue == null ? null : metricValue);
            }
        } catch (Exception e) {
            actualException = e;
        }
    }

    @When("^the metric is inserted$")
    public void theMetricIsInserted() throws Throwable {
        try {
            pugTSDB.upsert(metric);
        } catch (Exception e) {
            actualException = e;
        }
    }

    @Then("^no exceptions are thrown$")
    public void noExceptionsAreThrown() throws Throwable {
        assertNull(actualException);
    }

    @Then("^the metric is saved$")
    public void theMetricIsSaved() throws Throwable {
        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT \"id\", \"name\", \"type\" FROM metric WHERE \"id\"=?")) {
            statement.setInt(1, metric.getId());
            ResultSet resultSet = statement.executeQuery();

            assertTrue(resultSet.next());
            assertEquals(metric.getId(), (Integer) resultSet.getInt("id"));
            assertEquals(metric.getName(), resultSet.getString("name"));
            assertEquals(metric.getClass().getTypeName(), resultSet.getString("type"));
        }
    }

    @Then("^the tags are saved$")
    public void theTagsAreSaved() throws Throwable {
        for (Map.Entry<String, String> tag : metricTags.entrySet()) {
            try (Connection connection = pugTSDB.getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT * FROM tag WHERE \"name\"=? AND \"value\"=?")) {
                statement.setString(1, tag.getKey());
                statement.setString(2, tag.getValue());
                ResultSet resultSet = statement.executeQuery();

                assertTrue(resultSet.next());
            }
        }
    }

    @Then("^the relationship between the metric and the tags is saved$")
    public void theRelationshipBetweenTheMetricAndTheTagsIsSaved() throws Throwable {
        for (Map.Entry<String, String> tag : metricTags.entrySet()) {
            try (Connection connection = pugTSDB.getDataSource().getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT * FROM metric_tag WHERE \"metric_id\"=? AND \"tag_name\"=? AND \"tag_value\"=?")) {
                statement.setInt(1, metric.getId());
                statement.setString(2, tag.getKey());
                statement.setString(3, tag.getValue());
                ResultSet resultSet = statement.executeQuery();

                assertTrue(resultSet.next());
            }
        }
    }

    @Then("^the value is saved$")
    public void theValueIsSaved() throws Throwable {
        try (Connection connection = pugTSDB.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT \"metric_id\", \"timestamp\", \"value\" FROM data WHERE \"metric_id\"=?")) {
            statement.setInt(1, metric.getId());
            ResultSet resultSet = statement.executeQuery();

            assertTrue(resultSet.next());
            assertEquals(metric.getId(), (Integer) resultSet.getInt("metric_id"));
            assertEquals(metric.getTimestamp(), (Long) resultSet.getTimestamp("timestamp").getTime());

            byte[] expectedBytes = metric.getValueAsBytes();
            byte[] actualBytes = resultSet.getBytes("value");

            if (expectedBytes == null) {
                assertNull(actualBytes);
            } else {
                assertEquals(expectedBytes.length, actualBytes.length);

                for (int i = 0; i < expectedBytes.length; i++) {
                    assertEquals("byte[" + i + "] differ", expectedBytes[i], actualBytes[i]);
                }
            }
        }
    }

    @Then("^no tags are saved$")
    public void noTagsAreSaved() throws Throwable {
        try (Connection connection = pugTSDB.getDataSource().getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM tag");

            assertFalse(resultSet.next());
        }
    }

    @Then("^no relationship between the metric and the tags is saved$")
    public void noRelationshipBetweenTheMetricAndTheTagsIsSaved() throws Throwable {
        try (Connection connection = pugTSDB.getDataSource().getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM metric_tag");

            assertFalse(resultSet.next());
        }
    }

    @Then("^an illegal argument exception are thrown$")
    public void anIllegalArgumentExceptionAreThrown() throws Throwable {
        assertNotNull(actualException);
        assertTrue("Exception is not an " + PugIllegalArgumentException.class.getSimpleName() + ": " + actualException.getClass(),
                   actualException instanceof PugIllegalArgumentException);
    }
}
