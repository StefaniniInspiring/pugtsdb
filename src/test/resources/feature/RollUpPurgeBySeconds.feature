Feature: Roll-up purge by seconds tests

  Background: Default data for scenarios
    Given a double metric named "rollup.purge"
    And a SUM aggregation of double values

  Scenario: Points on expiration date
    Given a retention of 1 "seconds"
    And a rollup from 1 "millis" to 1 "seconds"
    And a rolled up point on "past" 1 "seconds" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "seconds" wont be purged

  Scenario: Points before expiration date
    Given a retention of 2 "seconds"
    And a rollup from 1 "millis" to 1 "seconds"
    And a rolled up point on "past" 1 "seconds" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "seconds" wont be purged

  Scenario: Points after expiration date
    Given a retention of 1 "seconds"
    And a rollup from 1 "millis" to 1 "seconds"
    And a rolled up point on "past" 2 "seconds" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 2 "seconds" will be purged
