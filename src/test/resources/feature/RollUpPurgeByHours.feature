Feature: Roll-up purge by hours tests

  Background: Default data for scenarios
    Given a double metric named "rollup.purge"
    And a SUM aggregation of double values

  Scenario: Points on expiration date
    Given a retention of 1 "hours"
    And a rollup from 1 "minutes" to 1 "hours"
    And a rolled up point on "past" 1 "hours" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "hours" wont be purged

  Scenario: Points before expiration date
    Given a retention of 2 "hours"
    And a rollup from 1 "minutes" to 1 "hours"
    And a rolled up point on "past" 1 "hours" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "hours" wont be purged

  Scenario: Points after expiration date
    Given a retention of 1 "hours"
    And a rollup from 1 "minutes" to 1 "hours"
    And a rolled up point on "past" 2 "hours" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 2 "hours" will be purged
