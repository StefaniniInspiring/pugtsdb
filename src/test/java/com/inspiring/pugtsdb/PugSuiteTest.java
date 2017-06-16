package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.creation.PugCreationFeature;
import com.inspiring.pugtsdb.upsertion.UpsertionFeature;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        PugCreationFeature.class,
        UpsertionFeature.class
})
public class PugSuiteTest {

}
