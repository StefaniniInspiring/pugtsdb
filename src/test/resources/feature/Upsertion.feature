Feature: Metric insertion tests

  Scenario: Insert a null metric
    When the metric is inserted
    Then an illegal argument exception are thrown

  #Boolean metric

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

  #Double metric

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

  #Long metric

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

  #String metric

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
    Then no exceptions are thrown
    And the metric is saved
    And the tags are saved
    And the relationship between the metric and the tags is saved
    And the value is saved