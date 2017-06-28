package com.inspiring.pugtsdb.selection;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = {
        "classpath:feature/SelectionByIdAggregationAndTimestamp.feature",
        "classpath:feature/SelectionByIdAndTimestamp.feature",
        "classpath:feature/SelectionOfRawPointsByIdAndTimestamp.feature"
})
public class SelectionFeature {

}
