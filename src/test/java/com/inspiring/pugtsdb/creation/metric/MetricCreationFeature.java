package com.inspiring.pugtsdb.creation.metric;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = "classpath:feature/MetricCreation.feature")
public class MetricCreationFeature {

}
