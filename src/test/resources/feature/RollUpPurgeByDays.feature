Feature: Roll-up purge by days tests

  Background: Default data for scenarios
    Given a double metric named "rollup.purge"
    And a SUM aggregation of double values

  Scenario: Points on expiration date
    Given a retention of 1 "days"
    And a rollup from 1 "hours" to 1 "days"
    And a rolled up point on "past" 1 "days" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "days" wont be purged

  Scenario: Points before expiration date
    Given a retention of 2 "days"
    And a rollup from 1 "hours" to 1 "days"
    And a rolled up point on "past" 1 "days" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "days" wont be purged

  Scenario: Points after expiration date
    Given a retention of 1 "days"
    And a rollup from 1 "hours" to 1 "days"
    And a rolled up point on "past" 2 "days" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 2 "days" will be purged

  Scenario: Mixed points date
    Given a retention of 2 "days"
    And a rollup from 1 "hours" to 1 "days"
    And a rolled up point on "past" 1 "days" with a double 123
    And a rolled up point on "past" 2 "days" with a double 123
    And a rolled up point on "past" 3 "days" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "days" wont be purged
    Then the rolled up point on "past" 2 "days" wont be purged
    Then the rolled up point on "past" 3 "days" will be purged
