Feature: Metric insertion tests of boolean points

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
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Update a Boolean metric
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "true"
    When the metric is created
    And the metric is inserted
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Boolean metric without tags
    Given the type "Boolean"
    And the name "bool.metric"
    And the timestamp 1496681817319
    And the value "true"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And no tags are saved
    And no relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Boolean metric without value
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved
