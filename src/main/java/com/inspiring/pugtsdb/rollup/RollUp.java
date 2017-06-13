package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.bean.MetricData;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.purge.AggregatedDataPurger;
import java.util.List;
import java.util.stream.Collectors;

public class RollUp implements Runnable {

    private final AggregatedDataPurger purger;
    private final String metricName;
    private final String period;
    private final Aggregation<Object> aggregation;
    private final Repositories repositories;

    private Long lastTimestamp = null;
    private Long nextTimestamp = null;

    public RollUp(String metricName,
                  Aggregation<Object> aggregation,
                  Retention retention,
                  String period,
                  Repositories repositories) {
        this.metricName = metricName;
        this.period = period;
        this.aggregation = aggregation;
        this.repositories = repositories;
        this.purger = new AggregatedDataPurger(repositories.getDataRepository(), retention, metricName, period);
    }

    @Override
    public void run() {
        nextTimestamp = null;// = truncateToTargetUnit(currentTimemillis)

        if (lastTimestamp == null) {
            //lastTimestamp = select max("timestamp") from data_target where "metric_id" in (select "id" from metric where "name" = metricName)

            if (lastTimestamp == null) {
                lastTimestamp = 0L;
            }
        }

        List<MetricData> sourceMetrics = repositories.getMetricRepository()
                .selectMetricDatasByNameAndTimestamp(metricName, period, lastTimestamp, nextTimestamp);

        sourceMetrics.stream()
                .collect(Collectors.groupingBy(MetricData::getId))
                .forEach((id, dataList) -> dataList.stream()
                        .collect(Collectors.groupingBy(data -> data.getTimestamp() /*.trucateTo(targetUnit)*/))
                        .forEach((timestamp, datas) -> {
                            MetricData metricData = datas.stream()
                                    .reduce((data1, data2) -> data1.merge(data2, aggregation))
                                    .orElse(null);
                            //insertMetricData()
                        }));

        lastTimestamp = nextTimestamp;

        purger.run();
    }
}
