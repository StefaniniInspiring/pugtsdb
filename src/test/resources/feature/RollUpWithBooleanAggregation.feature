Feature: Roll-up tests of boolean values

  Background: Default rollup for scenarios
    Given a boolean metric named "rollup.boolean"
    And a source granularity of 1 "millis"
    And a target granularity of 1 "seconds"
    And a retention of 1 "minutes"

  # AND aggregation

  Scenario: No points for AND aggregation
    Given an AND aggregation of boolean values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for AND aggregation
    Given an AND aggregation of boolean values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a boolean "true"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a boolean "true"

  Scenario: Many points for AND aggregation
    Given an AND aggregation of boolean values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a boolean "true"
    And a point on "past" "seconds" plus 20 "millis" with a boolean "false"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a boolean "false"

  # OR aggregation

  Scenario: No points for OR aggregation
    Given an OR aggregation of boolean values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for OR aggregation
    Given an OR aggregation of boolean values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a boolean "true"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a boolean "true"

  Scenario: Many points for OR aggregation
    Given an OR aggregation of boolean values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a boolean "true"
    And a point on "past" "seconds" plus 20 "millis" with a boolean "false"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a boolean "true"

