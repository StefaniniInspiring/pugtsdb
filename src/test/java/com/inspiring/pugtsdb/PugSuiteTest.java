package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.creation.PugCreationFeature;
import com.inspiring.pugtsdb.rollup.aggregation.RollUpAggregationFeature;
import com.inspiring.pugtsdb.rollup.granularity.RollUpGranularityFeature;
import com.inspiring.pugtsdb.rollup.listener.RollUpListenerFeature;
import com.inspiring.pugtsdb.selection.SelectionFeature;
import com.inspiring.pugtsdb.upsertion.UpsertionFeature;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        PugCreationFeature.class,
        UpsertionFeature.class,
        SelectionFeature.class,
        RollUpAggregationFeature.class,
        RollUpGranularityFeature.class,
//        RollUpPurgeFeature.class,
        RollUpListenerFeature.class
})
public class PugSuiteTest {

}
