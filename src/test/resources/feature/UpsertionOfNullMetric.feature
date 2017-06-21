Feature: Metric insertion tests

  Scenario: Insert a null metric
    When the metric is inserted
    Then an illegal argument exception are thrown
