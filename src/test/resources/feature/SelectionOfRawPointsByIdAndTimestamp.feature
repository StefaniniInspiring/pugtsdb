Feature: Metric raw points selection by ID and Timestamp

  Scenario: No points for metric ID
    Given the points for metric "other.metric":
      | 2017-06-28 15:00:00 | 99 |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns no metric points

  Scenario: No points for interval
    Given the points for metric "select.metric":
      | 2017-06-28 15:00:00 | 99 |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:01" and "2017-06-28 15:01:00"
    Then the select returns no metric points

  Scenario: One point for metric ID
    Given the points for metric "other.metric":
      | 2017-06-28 15:00:01 | 01 |
    And the points for metric "select.metric":
      | 2017-06-28 15:00:00 | 99 |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns a metric points for "select.metric"
    And the metric points contains 1 raw points
    And the metric points contains a raw point on "2017-06-28 15:00:00" with value 99

  Scenario: One point for interval
    Given the points for metric "select.metric":
      | 2017-06-28 14:59:59 | 01 |
      | 2017-06-28 15:00:00 | 00 |
      | 2017-06-28 15:01:00 | 00 |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns a metric points for "select.metric"
    And the metric points contains 1 raw points
    And the metric points contains a raw point on "2017-06-28 15:00:00" with value 00

  Scenario: Many points for ID and interval
    Given the points for metric "select.metric":
      | 2017-06-28 14:59:59 | -1 |
      | 2017-06-28 15:00:00 | 00 |
      | 2017-06-28 15:00:30 | 30 |
      | 2017-06-28 15:00:59 | 59 |
      | 2017-06-28 16:00:00 | 00 |
    When select points for metric "select.metric" ID between "2017-06-28 15:00:00" and "2017-06-28 16:00:00"
    Then the select returns a metric points for "select.metric"
    And the metric points contains 3 raw points
    And the metric points contains a raw point on "2017-06-28 15:00:00" with value 00
    And the metric points contains a raw point on "2017-06-28 15:00:30" with value 30
    And the metric points contains a raw point on "2017-06-28 15:00:59" with value 59
