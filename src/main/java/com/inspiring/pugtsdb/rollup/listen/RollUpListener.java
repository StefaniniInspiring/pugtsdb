package com.inspiring.pugtsdb.rollup.listen;

public interface RollUpListener<T> {

    void onRollUp(RollUpEvent<T> event);
}
