package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.rollup.aggregation.RollUpAggregationFeature;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
//        PugCreationFeature.class,
//        UpsertionFeature.class,
        RollUpAggregationFeature.class
})
public class PugSuiteTest {

}
