Feature: Metric points selection by Name, Tags, Aggregation and Timestamp

  Background: default data for tests
    Given a granularity of 1 "seconds"

  Scenario: No points for metric name and tags
    Given the points for metric "other.metric" with tag "foo" = "bar":
      | 2017-06-28 15:00:00 | sum | 99 |
    When select points for metric "select.metric" with tag "foo" = "baz" aggregated as "sum" between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns no metrics points

  Scenario: No points for aggregation
    Given the points for metric "select.metric" with tag "foo" = "bar":
      | 2017-06-28 15:00:00 | sum | 99 |
    When select points for metric "select.metric" with tag "foo" = "bar" aggregated as "avg" between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns no metrics points

  Scenario: No points for interval
    Given the points for metric "select.metric" with tag "foo" = "bar":
      | 2017-06-28 15:00:00 | sum | 99 |
    When select points for metric "select.metric" with tag "foo" = "bar" aggregated as "sum" between "2017-06-28 15:00:01" and "2017-06-28 15:01:00"
    Then the select returns no metrics points

  Scenario: One point for metric name and tags
    Given the points for metric "other.metric" with tag "foo" = "bar":
      | 2017-06-28 15:00:01 | sum | 01 |
    And the points for metric "select.metric" with tag "foo" = "baz":
      | 2017-06-28 15:00:00 | sum | 99 |
    When select points for metric "select.metric" with tag "foo" = "baz" aggregated as "sum" between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns 1 metric points for "select.metric" with tag "foo" = "baz"
    And the select returns 0 metric points for "select.metric" with tag "foo" = "bar"
    And the metric "select.metric" with tag "foo" = "baz" contains 1 points aggregated as "sum"
    And the metric "select.metric" with tag "foo" = "baz" contains a point aggregated as "sum" on "2017-06-28 15:00:00" with value 99

  Scenario: One point for aggregation
    Given the points for metric "select.metric" with tag "foo" = "bar":
      | 2017-06-28 15:00:00 | avg | 01 |
      | 2017-06-28 15:00:00 | sum | 99 |
    When select points for metric "select.metric" with tag "foo" = "bar" aggregated as "sum" between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns 1 metric points for "select.metric" with tag "foo" = "bar"
    And the metric "select.metric" with tag "foo" = "bar" contains 1 points aggregated as "sum"
    And the metric "select.metric" with tag "foo" = "bar" contains a point aggregated as "sum" on "2017-06-28 15:00:00" with value 99

  Scenario: One point for interval
    Given the points for metric "select.metric" with tag "foo" = "bar":
      | 2017-06-28 14:59:59 | sum | 01 |
      | 2017-06-28 15:00:00 | sum | 00 |
      | 2017-06-28 15:01:00 | sum | 00 |
    When select points for metric "select.metric" with tag "foo" = "bar" aggregated as "sum" between "2017-06-28 15:00:00" and "2017-06-28 15:01:00"
    Then the select returns 1 metric points for "select.metric" with tag "foo" = "bar"
    And the metric "select.metric" with tag "foo" = "bar" contains 1 points aggregated as "sum"
    And the metric "select.metric" with tag "foo" = "bar" contains a point aggregated as "sum" on "2017-06-28 15:00:00" with value 00

  Scenario: Many points for name, tags, aggregation and interval
    Given the points for metric "select.metric" with tag "foo" = "bar":
      | 2017-06-28 14:59:59 | sum | -1 |
      | 2017-06-28 15:00:00 | sum | 00 |
      | 2017-06-28 15:00:30 | sum | 30 |
      | 2017-06-28 15:00:59 | sum | 59 |
      | 2017-06-28 16:00:00 | sum | 00 |
    When select points for metric "select.metric" with tag "foo" = "bar" aggregated as "sum" between "2017-06-28 15:00:00" and "2017-06-28 16:00:00"
    Then the select returns 1 metric points for "select.metric" with tag "foo" = "bar"
    And the metric "select.metric" with tag "foo" = "bar" contains 3 points aggregated as "sum"
    And the metric "select.metric" with tag "foo" = "bar" contains a point aggregated as "sum" on "2017-06-28 15:00:00" with value 00
    And the metric "select.metric" with tag "foo" = "bar" contains a point aggregated as "sum" on "2017-06-28 15:00:30" with value 30
    And the metric "select.metric" with tag "foo" = "bar" contains a point aggregated as "sum" on "2017-06-28 15:00:59" with value 59
      