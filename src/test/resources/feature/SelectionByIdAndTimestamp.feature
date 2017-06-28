Feature: Metric points selection by ID and Timestamp

  Background: default data for tests
    Given a granularity of 1 "seconds"

  Scenario: No points for metric ID
    Given the points for metric "other.metric":
      | 2017-06-28 15:00:00 | sum | 99 |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns no metric points

  Scenario: No points for interval
    Given the points for metric "select.metric":
      | 2017-06-28 15:00:00 | sum | 99 |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:01" and "2017-06-28 15:01:00"
    Then the select returns no metric points

  Scenario: One point for metric ID
    Given the points for metric "other.metric":
      | 2017-06-28 15:00:01 | sum | 01 |
    And the points for metric "select.metric":
      | 2017-06-28 15:00:00 | sum | 99 |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns a metric points for "select.metric"
    And the metric points contains 1 points aggregated as "sum"
    And the metric points contains a point aggregated as "sum" on "2017-06-28 15:00:00" with value 99

  Scenario: One point for interval
    Given the points for metric "select.metric":
      | 2017-06-28 14:59:59 | sum | 01 |
      | 2017-06-28 15:00:00 | sum | 00 |
      | 2017-06-28 15:01:00 | sum | 00 |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns a metric points for "select.metric"
    And the metric points contains 1 points aggregated as "sum"
    And the metric points contains a point aggregated as "sum" on "2017-06-28 15:00:00" with value 00

  Scenario: Many points for ID and interval
    Given the points for metric "select.metric":
      | 2017-06-28 14:59:59 | sum | -1   |
      | 2017-06-28 15:00:00 | sum | 00   |
      | 2017-06-28 15:00:30 | sum | 30   |
      | 2017-06-28 15:00:59 | sum | 59   |
      | 2017-06-28 16:00:00 | sum | 00   |
      | 2017-06-28 14:59:59 | avg | -1   |
      | 2017-06-28 15:00:00 | avg | 1000 |
      | 2017-06-28 15:00:30 | avg | 1030 |
      | 2017-06-28 15:00:59 | avg | 1059 |
      | 2017-06-28 16:00:00 | avg | 00   |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:00" and "2017-06-28 16:00:00"
    Then the select returns a metric points for "select.metric"
    And the metric points contains 3 points aggregated as "sum"
    And the metric points contains a point aggregated as "sum" on "2017-06-28 15:00:00" with value 00
    And the metric points contains a point aggregated as "sum" on "2017-06-28 15:00:30" with value 30
    And the metric points contains a point aggregated as "sum" on "2017-06-28 15:00:59" with value 59
    And the metric points contains 3 points aggregated as "avg"
    And the metric points contains a point aggregated as "avg" on "2017-06-28 15:00:00" with value 1000
    And the metric points contains a point aggregated as "avg" on "2017-06-28 15:00:30" with value 1030
    And the metric points contains a point aggregated as "avg" on "2017-06-28 15:00:59" with value 1059
      