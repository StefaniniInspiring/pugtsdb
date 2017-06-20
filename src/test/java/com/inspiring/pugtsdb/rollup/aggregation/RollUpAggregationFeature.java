package com.inspiring.pugtsdb.rollup.aggregation;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = {
        "classpath:feature/RollUpWithBooleanAggregation.feature"
})
public class RollUpAggregationFeature {

}
