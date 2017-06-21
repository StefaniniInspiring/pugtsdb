Feature: Metric insertion tests

  Scenario: Insert a Long metric with min value
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "-9223372036854775808"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Long metric with max value
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "9223372036854775807"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Long metric with mid value
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "1"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Update a Long metric
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "1"
    When the metric is created
    And the metric is inserted
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Long metric without tags
    Given the type "Long"
    And the name "long.metric"
    And the timestamp 1496681817319
    And the value "1"
    When the metric is created
    And the metric is inserted
    Then no exceptions are thrown
    And the metric is saved
    And no tags are saved
    And no relationship between the metric and the tags is saved
    And the value is saved

  Scenario: Insert a Long metric without value
    Given the type "Long"
    And the name "long.metric"
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
