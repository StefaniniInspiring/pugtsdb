Feature: Metric insertion tests of null point value

  Scenario: Insert a null metric
    When the metric is inserted
    Then an illegal argument exception are thrown
