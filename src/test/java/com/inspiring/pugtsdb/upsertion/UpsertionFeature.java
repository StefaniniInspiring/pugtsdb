package com.inspiring.pugtsdb.upsertion;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = "classpath:feature/Upsertion.feature")
public class UpsertionFeature {

}
