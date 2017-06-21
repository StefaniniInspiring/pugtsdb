Feature: Roll-up purge by minutes tests

  Background: Default data for scenarios
    Given a double metric named "rollup.purge"
    And a SUM aggregation of double values

  Scenario: Points on expiration date
    Given a retention of 1 "minutes"
    And a rollup from 1 "seconds" to 1 "minutes"
    And a rolled up point on "past" 1 "minutes" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "minutes" wont be purged

  Scenario: Points before expiration date
    Given a retention of 2 "minutes"
    And a rollup from 1 "seconds" to 1 "minutes"
    And a rolled up point on "past" 1 "minutes" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "minutes" wont be purged

  Scenario: Points after expiration date
    Given a retention of 1 "minutes"
    And a rollup from 1 "seconds" to 1 "minutes"
    And a rolled up point on "past" 2 "minutes" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 2 "minutes" will be purged
