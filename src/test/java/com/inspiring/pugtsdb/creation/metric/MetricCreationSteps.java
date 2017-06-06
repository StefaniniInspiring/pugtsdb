package com.inspiring.pugtsdb.creation.metric;

import com.inspiring.pugtsdb.exception.PugConversionException;
import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import com.inspiring.pugtsdb.metric.BooleanMetric;
import com.inspiring.pugtsdb.metric.DoubleMetric;
import com.inspiring.pugtsdb.metric.LongMetric;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.metric.StringMetric;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class MetricCreationSteps {

    private String metricType;
    private String metricName;
    private Map<String, String> metricTags;
    private Long metricTimestamp;
    private Object metricValue;
    private byte[] metricBytes;

    private Metric<?> actualMetric;
    private Exception actualException;

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
        if (value != null) {
            switch (metricType) {
                case "Boolean":
                    metricValue = Boolean.valueOf(value);
                    break;
                case "Double":
                    metricValue = Double.valueOf(value);
                    break;
                case "Long":
                    metricValue = Long.valueOf(value);
                    break;
                default:
                    metricValue = value;
            }
        }
    }

    @Given("^the bytes empty$")
    public void theBytesEmpty() throws Throwable {
        metricBytes = new byte[0];
    }

    @When("^the metric is created$")
    public void theMetricIsCreated() throws Throwable {
        try {
            switch (metricType) {
                case "Boolean":
                    actualMetric = new BooleanMetric(metricName, metricTags, metricTimestamp, (Boolean) metricValue);
                    break;
                case "Double":
                    actualMetric = new DoubleMetric(metricName, metricTags, metricTimestamp, (Double) metricValue);
                    break;
                case "Long":
                    actualMetric = new LongMetric(metricName, metricTags, metricTimestamp, (Long) metricValue);
                    break;
                default:
                    actualMetric = new StringMetric(metricName, metricTags, metricTimestamp, (String) metricValue);
            }
        } catch (Exception e) {
            actualException = e;
        }
    }

    @When("^the metric is created with bytes$")
    public void theMetricIsCreatedWithBytes() throws Throwable {
        try {
            switch (metricType) {
                case "Boolean":
                    actualMetric = new BooleanMetric(metricName, metricTags, metricTimestamp, metricBytes);
                    break;
                case "Double":
                    actualMetric = new DoubleMetric(metricName, metricTags, metricTimestamp, metricBytes);
                    break;
                case "Long":
                    actualMetric = new LongMetric(metricName, metricTags, metricTimestamp, metricBytes);
                    break;
                default:
                    actualMetric = new StringMetric(metricName, metricTags, metricTimestamp, metricBytes);
            }
        } catch (Exception e) {
            actualException = e;
        }
    }

    @Then("^the metric creation is successful$")
    public void theMetricCreationIsSuccessful() throws Throwable {
        assertNull(actualException);
        assertNotNull(actualMetric);

        assertNotNull(actualMetric.getId());
        assertEquals(metricName, actualMetric.getName());

        if (metricTags != null) {
            assertEquals(metricTags, actualMetric.getTags());
        } else {
            assertEquals(emptyMap(), actualMetric.getTags());
        }

        if (metricTimestamp != null) {
            assertEquals(metricTimestamp, actualMetric.getTimestamp());
        } else {
            assertNotNull(actualMetric.getTimestamp());
        }

        if (metricValue != null) {
            assertEquals(metricValue, actualMetric.getValue());
        } else if (metricBytes != null) {
            byte[] actualBytes = actualMetric.getValueAsBytes();

            assertEquals(metricBytes.length, actualBytes.length);

            for (int i = 0; i < metricBytes.length; i++) {
                assertEquals("byte[" + i + "] differ", metricBytes[i], actualBytes[i]);
            }
        } else {
            assertNull(actualMetric.getValue());
            assertNull(actualMetric.getValueAsBytes());
        }
    }

    @Then("^an illegal argument exception are thrown$")
    public void anIllegalArgumentExceptionAreThrown() throws Throwable {
        assertNotNull(actualException);
        assertTrue("Exception is not an " + PugIllegalArgumentException.class.getSimpleName() + ": " + actualException.getClass(),
                   actualException instanceof PugIllegalArgumentException);
    }

    @Then("^a conversion exception are thrown$")
    public void aConversionExceptionAreThrown() throws Throwable {
        assertNotNull(actualException);
        assertTrue("Exception is not an " + PugConversionException.class.getSimpleName() + ": " + actualException.getClass(),
                   actualException instanceof PugConversionException);
    }
}
