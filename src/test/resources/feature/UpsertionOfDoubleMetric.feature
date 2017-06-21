Feature: Metric insertion tests

  Scenario: Insert a Double metric with min value
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "0x0.0000000000001P-1022"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Double metric with max value
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "0x1.fffffffffffffP+1023"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Double metric with mid value
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "99.99D"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Update a Double metric
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "99.99D"
    When the metric is created
    And the metric is inserted
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Double metric without tags
    Given the type "Double"
    And the name "double.metric"
    And the timestamp 1496681817319
    And the value "99.99D"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And no tags are saved
    And no relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Double metric without value
    Given the type "Double"
    And the name "double.metric"
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
