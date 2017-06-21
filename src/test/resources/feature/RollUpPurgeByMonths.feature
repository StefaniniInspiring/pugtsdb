Feature: Roll-up purge by months tests

  Background: Default data for scenarios
    Given a double metric named "rollup.purge"
    And a SUM aggregation of double values

  Scenario: Points on expiration date
    Given a retention of 1 "months"
    And a rollup from 1 "days" to 1 "months"
    And a rolled up point on "past" 1 "months" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "months" wont be purged

  Scenario: Points before expiration date
    Given a retention of 2 "months"
    And a rollup from 1 "days" to 1 "months"
    And a rolled up point on "past" 1 "months" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 1 "months" wont be purged

  Scenario: Points after expiration date
    Given a retention of 1 "months"
    And a rollup from 1 "days" to 1 "months"
    And a rolled up point on "past" 2 "months" with a double 123
    When the rollup executes
    Then the rolled up point on "past" 2 "months" will be purged
