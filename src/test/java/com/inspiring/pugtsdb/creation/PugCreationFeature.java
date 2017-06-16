package com.inspiring.pugtsdb.creation;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = "classpath:feature/PugCreation.feature")
public class PugCreationFeature {

}
