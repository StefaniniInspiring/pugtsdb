Feature: Roll-up tests of string values

  Background: Default rollup for scenarios
    Given a string metric named "rollup.string"
    And a source granularity of 1 "millis"
    And a target granularity of 1 "seconds"
    And a retention of 1 "minutes"

  # SUM aggregation

  Scenario: No points for SUM aggregation
    Given a SUM aggregation of string values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for SUM aggregation
    Given a SUM aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a string "foo"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a string "foo"

  Scenario: Many points for SUM aggregation
    Given a SUM aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a string "foo"
    And a point on "past" "seconds" plus 20 "millis" with a string "bar"
    And a point on "past" "seconds" plus 30 "millis" with a string "baz"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a string "foobarbaz"

  Scenario: Many points including null for SUM aggregation
    Given a SUM aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a string "foo"
    And a point on "past" "seconds" plus 20 "millis" with a null value
    And a point on "past" "seconds" plus 30 "millis" with a string "bar"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a string "foobar"

  Scenario: Many points all null for SUM aggregation
    Given a SUM aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a null value
    And a point on "past" "seconds" plus 20 "millis" with a null value
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with null value

  # MIN aggregation

  Scenario: No points for MIN aggregation
    Given a MIN aggregation of string values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for MIN aggregation
    Given a MIN aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a string "apples"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a string "apples"

  Scenario: Many points for MIN aggregation
    Given a MIN aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a string "oranges"
    And a point on "past" "seconds" plus 20 "millis" with a string "apples"
    And a point on "past" "seconds" plus 30 "millis" with a string "grapes"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a string "apples"

  Scenario: Many points including null for MIN aggregation
    Given a MIN aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a string "oranges"
    And a point on "past" "seconds" plus 20 "millis" with a null value
    And a point on "past" "seconds" plus 30 "millis" with a string "apples"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a string "apples"

  Scenario: Many points all null for MIN aggregation
    Given a MIN aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a null value
    And a point on "past" "seconds" plus 20 "millis" with a null value
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with null value

  # MAX aggregation

  Scenario: No points for MAX aggregation
    Given a MAX aggregation of string values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for MAX aggregation
    Given a MAX aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a string "apples"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a string "apples"

  Scenario: Many points for MAX aggregation
    Given a MAX aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a string "oranges"
    And a point on "past" "seconds" plus 20 "millis" with a string "apples"
    And a point on "past" "seconds" plus 30 "millis" with a string "grapes"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a string "oranges"

  Scenario: Many points including null for MAX aggregation
    Given a MAX aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a string "oranges"
    And a point on "past" "seconds" plus 20 "millis" with a null value
    And a point on "past" "seconds" plus 30 "millis" with a string "apples"
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a string "oranges"

  Scenario: Many points all null for MAX aggregation
    Given a MAX aggregation of string values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a null value
    And a point on "past" "seconds" plus 20 "millis" with a null value
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with null value

