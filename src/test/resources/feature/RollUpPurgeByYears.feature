Feature: Roll-up purge by years tests

  Background: Default data for scenarios
    Given a double metric named "rollup.purge"
    And a SUM aggregation of double values

  Scenario: Points on expiration date
    Given a retention of 1 "years"
    And a rollup from 1 "months" to 1 "years"
    And a rolled up point on "past" 1 "years" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "years" wont be purged

  Scenario: Points before expiration date
    Given a retention of 2 "years"
    And a rollup from 1 "months" to 1 "years"
    And a rolled up point on "past" 1 "years" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "years" wont be purged

  Scenario: Points after expiration date
    Given a retention of 1 "years"
    And a rollup from 1 "months" to 1 "years"
    And a rolled up point on "past" 2 "years" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 2 "years" will be purged
