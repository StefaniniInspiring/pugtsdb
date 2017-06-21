package com.inspiring.pugtsdb.upsertion;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = {
        "classpath:feature/UpsertionOfDoubleMetric.feature",
        "classpath:feature/UpsertionOfLongMetric.feature",
        "classpath:feature/UpsertionOfStringMetric.feature",
        "classpath:feature/UpsertionOfBooleanMetric.feature",
        "classpath:feature/UpsertionOfNullMetric.feature"
})
public class UpsertionFeature {

}
