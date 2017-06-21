Feature: Roll-up tests focused on granularity

  Background: Default rollup for scenarios
    Given a double metric named "rollup.granularity"
    And a SUM aggregation of double values
    And a retention of 2 "years"
    And a rollup from 1 "millis" to 1 "seconds"
    And a rollup from 1 "seconds" to 1 "minutes"
    And a rollup from 1 "minutes" to 1 "hours"
    And a rollup from 1 "hours" to 1 "days"
    And a rollup from 1 "days" to 1 "months"
    And a rollup from 1 "months" to 1 "years"

  Scenario: Raw points on future seconds
    Given a point on "future" "seconds" with a double 123
    When the rollups executes
    Then no points are rolled up by 1 "seconds"
    And no points are rolled up by 1 "minutes"
    And no points are rolled up by 1 "hours"
    And no points are rolled up by 1 "days"
    And no points are rolled up by 1 "months"
    And no points are rolled up by 1 "years"

  Scenario: Raw points on current seconds
    Given a point on "current" "seconds" with a double 123
    When the rollups executes
    Then no points are rolled up by 1 "seconds"
    And no points are rolled up by 1 "minutes"
    And no points are rolled up by 1 "hours"
    And no points are rolled up by 1 "days"
    And no points are rolled up by 1 "months"
    And no points are rolled up by 1 "years"

  Scenario: Raw points on past seconds
    Given a point on "past" "seconds" with a double 123
    When the rollups executes
    Then a point on "past" "seconds" will be rolled up by 1 "seconds" with a double 123
    And no points are rolled up by 1 "minutes"
    And no points are rolled up by 1 "hours"
    And no points are rolled up by 1 "days"
    And no points are rolled up by 1 "months"
    And no points are rolled up by 1 "years"

  Scenario: Raw points on past hours
    Given a point on "past" "hours" with a double 123
    When the rollups executes
    Then a point on "past" "hours" will be rolled up by 1 "seconds" with a double 123
    And a point on "past" "hours" will be rolled up by 1 "minutes" with a double 123
    And a point on "past" "hours" will be rolled up by 1 "hours" with a double 123
    And no points are rolled up by 1 "days"
    And no points are rolled up by 1 "months"
    And no points are rolled up by 1 "years"

  Scenario: Raw points on past days
    Given a point on "past" "days" with a double 123
    When the rollups executes
    Then a point on "past" "days" will be rolled up by 1 "seconds" with a double 123
    And a point on "past" "days" will be rolled up by 1 "minutes" with a double 123
    And a point on "past" "days" will be rolled up by 1 "hours" with a double 123
    And a point on "past" "days" will be rolled up by 1 "days" with a double 123
    And no points are rolled up by 1 "months"
    And no points are rolled up by 1 "years"

  Scenario: Raw points on past months
    Given a point on "past" "months" with a double 123
    When the rollups executes
    Then a point on "past" "months" will be rolled up by 1 "seconds" with a double 123
    And a point on "past" "months" will be rolled up by 1 "minutes" with a double 123
    And a point on "past" "months" will be rolled up by 1 "hours" with a double 123
    And a point on "past" "months" will be rolled up by 1 "days" with a double 123
    And a point on "past" "months" will be rolled up by 1 "months" with a double 123
    And no points are rolled up by 1 "years"

  Scenario: Raw points on past years
    Given a point on "past" "years" with a double 123
    When the rollups executes
    Then a point on "past" "years" will be rolled up by 1 "seconds" with a double 123
    And a point on "past" "years" will be rolled up by 1 "minutes" with a double 123
    And a point on "past" "years" will be rolled up by 1 "hours" with a double 123
    And a point on "past" "years" will be rolled up by 1 "days" with a double 123
    And a point on "past" "years" will be rolled up by 1 "months" with a double 123
    And a point on "past" "years" will be rolled up by 1 "years" with a double 123
