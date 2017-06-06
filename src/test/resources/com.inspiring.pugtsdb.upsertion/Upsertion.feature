Feature: Metric insertion tests

  Scenario: Insert a Boolean metric
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "true"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags is saved
    And the relationship between the metric and the tags is saved
    And the value is saved