package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.creation.PugCreationFeature;
import com.inspiring.pugtsdb.rollup.aggregation.RollUpAggregationFeature;
import com.inspiring.pugtsdb.rollup.granularity.RollUpGranularityFeature;
import com.inspiring.pugtsdb.rollup.purge.RollUpPurgeFeature;
import com.inspiring.pugtsdb.upsertion.UpsertionFeature;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        PugCreationFeature.class,
        UpsertionFeature.class,
        RollUpAggregationFeature.class,
        RollUpGranularityFeature.class,
        RollUpPurgeFeature.class
})
public class PugSuiteTest {

}
