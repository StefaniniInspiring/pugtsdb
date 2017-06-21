package com.inspiring.pugtsdb.rollup.purge;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = {
        "classpath:feature/RollUpPurgeBySeconds.feature",
        "classpath:feature/RollUpPurgeByMinutes.feature",
        "classpath:feature/RollUpPurgeByHours.feature",
        "classpath:feature/RollUpPurgeByDays.feature",
        "classpath:feature/RollUpPurgeByMonths.feature",
        "classpath:feature/RollUpPurgeByYears.feature"
})
public class RollUpPurgeFeature {

}
