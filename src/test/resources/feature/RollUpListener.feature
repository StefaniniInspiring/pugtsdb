Feature: Roll-up listener tests

  Background:  Default data for scenarios
    Given a double metric named "rollup.listener"
    And a SUM aggregation of double values
    And a retention of 1 "minutes"
    And a rollup from 1 "millis" to 1 "seconds"
    And a rollup listener

  Scenario: Roll-up runs without listener added
    Given a point on "past" "seconds" plus 1 "millis" with a double 123
    When the rollup executes
    Then the listener wont be called

  Scenario: Roll-up runs with listener added but without points to roll
    When the listener is added to rollup
    And the rollup executes
    Then the listener wont be called

  Scenario: Roll-up runs with listener added
    Given a point on "past" "seconds" plus 1 "millis" with a double 123
    When the listener is added to rollup
    And the rollup executes
    Then the listener will be called
    And the event will have a metric name of "rollup.listener"
    And the event will have a aggregation name of "sum"
    And the event will have a source granularity of 1 "millis"
    And the event will have a target granularity of 1 "seconds"

