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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MetricCreationSteps {

    private String metricType;
    private String metricName;
    private Map<String, String> metricTags;
    private Long metricTimestamp;
    private String metricValue;
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
        metricValue = value;
    }

    @Given("^the name null$")
    public void theNameNull() throws Throwable {
        metricName = null;
    }

    @Given("^the tags null$")
    public void theTagsNull() throws Throwable {
        metricTags = null;
    }

    @Given("^the timestamp null$")
    public void theTimestampNull() throws Throwable {
        metricTimestamp = null;
    }

    @Given("^the value null$")
    public void theValueNull() throws Throwable {
        metricValue = null;
    }

    @Given("^the bytes null$")
    public void theBytesNull() throws Throwable {
        metricBytes = null;
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
                    actualMetric = new BooleanMetric(metricName, metricTags, metricTimestamp, metricValue == null ? null : Boolean.parseBoolean(metricValue));
                    break;
                case "Double":
                    actualMetric = new DoubleMetric(metricName, metricTags, metricTimestamp, metricValue == null ? null : Double.parseDouble(metricValue));
                    break;
                case "Long":
                    actualMetric = new LongMetric(metricName, metricTags, metricTimestamp, metricValue == null ? null : Long.parseLong(metricValue));
                    break;
                default:
                    actualMetric = new StringMetric(metricName, metricTags, metricTimestamp, metricValue == null ? null : metricValue);
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
