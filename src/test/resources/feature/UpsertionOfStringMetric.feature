Feature: Metric insertion tests of string points

  Scenario: Insert a String metric
    Given the type "String"
    And the name "string.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "01234567891abcdefghijklmnopqrstuvxywzABCDEFGHIJKLMNOPQRSTUVXYWZ.?!@#$%&*=\\/"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a String metric with empty value
    Given the type "String"
    And the name "string.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value ""
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Update a String metric
    Given the type "String"
    And the name "string.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "01234567891abcdefghijklmnopqrstuvxywzABCDEFGHIJKLMNOPQRSTUVXYWZ.?!@#$%&*=\\/"
    When the metric is created
    And the metric is inserted
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a String metric without tags
    Given the type "String"
    And the name "string.metric"
    And the timestamp 1496681817319
    And the value "01234567891abcdefghijklmnopqrstuvxywzABCDEFGHIJKLMNOPQRSTUVXYWZ.?!@#$%&*=\\/"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And no tags are saved
    And no relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a String metric without value
    Given the type "String"
    And the name "string.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    When the metric is created
    And the metric is inserted
    Then an illegal argument exception are thrown