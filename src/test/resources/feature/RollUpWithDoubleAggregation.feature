Feature: Roll-up tests of double values

  Background: Default rollup for scenarios
    Given a double metric named "rollup.double"
    And a source granularity of 1 "millis"
    And a target granularity of 1 "seconds"
    And a retention of 1 "minutes"

  # SUM aggregation

  Scenario: No points for SUM aggregation
    Given a SUM aggregation of double values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for SUM aggregation
    Given a SUM aggregation of double values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a double 123
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a double 123

  Scenario: Many points for SUM aggregation
    Given a SUM aggregation of double values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a double 100
    And a point on "past" "seconds" plus 20 "millis" with a double 20
    And a point on "past" "seconds" plus 30 "millis" with a double 3
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a double 123

  # MIN aggregation

  Scenario: No points for MIN aggregation
    Given a MIN aggregation of double values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for MIN aggregation
    Given a MIN aggregation of double values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a double 123
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a double 123

  Scenario: Many points for MIN aggregation
    Given a MIN aggregation of double values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a double 100
    And a point on "past" "seconds" plus 20 "millis" with a double 20
    And a point on "past" "seconds" plus 30 "millis" with a double 3
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a double 3

  # MAX aggregation

  Scenario: No points for MAX aggregation
    Given a MAX aggregation of double values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for MAX aggregation
    Given a MAX aggregation of double values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a double 123
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a double 123

  Scenario: Many points for MAX aggregation
    Given a MAX aggregation of double values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a double 100
    And a point on "past" "seconds" plus 20 "millis" with a double 20
    And a point on "past" "seconds" plus 30 "millis" with a double 3
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a double 100

  # AVG aggregation

  Scenario: No points for AVG aggregation
    Given an AVG aggregation of double values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for AVG aggregation
    Given an AVG aggregation of double values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a double 123
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a double 123

  Scenario: Many points for AVG aggregation
    Given an AVG aggregation of double values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a double 100
    And a point on "past" "seconds" plus 20 "millis" with a double 20
    And a point on "past" "seconds" plus 30 "millis" with a double 3
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a double 41
