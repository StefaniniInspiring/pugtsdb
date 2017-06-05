package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.creation.metric.MetricCreationFeature;
import com.inspiring.pugtsdb.creation.pug.PugCreationFeature;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        MetricCreationFeature.class,
        PugCreationFeature.class,
//        UpsertionFeature.class
})
public class PugTestSuite {

}
