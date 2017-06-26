package com.inspiring.pugtsdb.rollup.listen;

public interface RollUpListener {

    void onRollUp(RollUpEvent event);
}
