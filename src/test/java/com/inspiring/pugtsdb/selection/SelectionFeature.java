package com.inspiring.pugtsdb.selection;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = {
        "classpath:feature/SelectionByIdAggregationAndTimestamp.feature",
        "classpath:feature/SelectionByIdAndTimestamp.feature",
        "classpath:feature/SelectionOfRawPointsByIdAndTimestamp.feature",
        "classpath:feature/SelectionByNameTagsAggregationAndTimestamp.feature",
        "classpath:feature/SelectionByNameTagsAndTimestamp.feature",
        "classpath:feature/SelectionOfRawPointsByNameTagsAndTimestamp.feature",
})
public class SelectionFeature {

}
