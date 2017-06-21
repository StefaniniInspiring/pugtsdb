Feature: Roll-up tests of long values

  Background: Default rollup for scenarios
    Given a long metric named "rollup.long"
    And a source granularity of 1 "millis"
    And a target granularity of 1 "seconds"
    And a retention of 1 "minutes"

  # SUM aggregation

  Scenario: No points for SUM aggregation
    Given a SUM aggregation of long values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for SUM aggregation
    Given a SUM aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a long 123
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 123

  Scenario: Many points for SUM aggregation
    Given a SUM aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a long 100
    And a point on "past" "seconds" plus 20 "millis" with a long 20
    And a point on "past" "seconds" plus 30 "millis" with a long 3
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 123

  Scenario: Many points including null for SUM aggregation
    Given a SUM aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a long 10
    And a point on "past" "seconds" plus 20 "millis" with a null value
    And a point on "past" "seconds" plus 30 "millis" with a long 20
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 30

  Scenario: Many points all null for SUM aggregation
    Given a SUM aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a null value
    And a point on "past" "seconds" plus 20 "millis" with a null value
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with null value

  # MIN aggregation

  Scenario: No points for MIN aggregation
    Given a MIN aggregation of long values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for MIN aggregation
    Given a MIN aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a long 123
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 123

  Scenario: Many points for MIN aggregation
    Given a MIN aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a long 100
    And a point on "past" "seconds" plus 20 "millis" with a long 20
    And a point on "past" "seconds" plus 30 "millis" with a long 3
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 3

  Scenario: Many points including null for MIN aggregation
    Given a MIN aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a long 10
    And a point on "past" "seconds" plus 20 "millis" with a null value
    And a point on "past" "seconds" plus 30 "millis" with a long 20
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 10

  Scenario: Many points all null for MIN aggregation
    Given a MIN aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a null value
    And a point on "past" "seconds" plus 20 "millis" with a null value
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with null value

  # MAX aggregation

  Scenario: No points for MAX aggregation
    Given a MAX aggregation of long values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for MAX aggregation
    Given a MAX aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a long 123
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 123

  Scenario: Many points for MAX aggregation
    Given a MAX aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a long 100
    And a point on "past" "seconds" plus 20 "millis" with a long 20
    And a point on "past" "seconds" plus 30 "millis" with a long 3
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 100

  Scenario: Many points including null for MAX aggregation
    Given a MAX aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a long 10
    And a point on "past" "seconds" plus 20 "millis" with a null value
    And a point on "past" "seconds" plus 30 "millis" with a long 20
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 20

  Scenario: Many points all null for MAX aggregation
    Given a MAX aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a null value
    And a point on "past" "seconds" plus 20 "millis" with a null value
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with null value

  # AVG aggregation

  Scenario: No points for AVG aggregation
    Given an AVG aggregation of long values
    And a rollup instance
    When the rollup executes
    Then no points are rolled up

  Scenario: Single point for AVG aggregation
    Given an AVG aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 1 "millis" with a long 123
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 123

  Scenario: Many points for AVG aggregation
    Given an AVG aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a long 100
    And a point on "past" "seconds" plus 20 "millis" with a long 20
    And a point on "past" "seconds" plus 30 "millis" with a long 3
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 41

  Scenario: Many points including null for AVG aggregation
    Given an AVG aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a long 10
    And a point on "past" "seconds" plus 20 "millis" with a null value
    And a point on "past" "seconds" plus 30 "millis" with a long 20
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with a long 10

  Scenario: Many points all null for AVG aggregation
    Given an AVG aggregation of long values
    And a rollup instance
    And a point on "past" "seconds" plus 10 "millis" with a null value
    And a point on "past" "seconds" plus 20 "millis" with a null value
    When the rollup executes
    Then a point on "past" "seconds" will be rolled up with null value
