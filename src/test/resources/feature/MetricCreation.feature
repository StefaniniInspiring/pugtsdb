Feature: Metrics instantiation tests

  #Boolean metric tests

  Scenario: Create a Boolean metric with valid arguments
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "true"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Boolean metric with null name
    Given the type "Boolean"
    And the name null
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "true"
    When the metric is created
    Then an illegal argument exception are thrown

  Scenario: Create a Boolean metric with empty name
    Given the type "Boolean"
    And the name ""
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "true"
    When the metric is created
    Then an illegal argument exception are thrown

  Scenario: Create a Boolean metric with null tags
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags null
    And the timestamp 1496681817319
    And the value "true"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Boolean metric with empty tags
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags:
      |  |  |
    And the timestamp 1496681817319
    And the value "true"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Boolean metric with null timestamp
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp null
    And the value "true"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Boolean metric with null value
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value null
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Boolean metric with null bytes
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the bytes null
    When the metric is created with bytes
    Then the metric creation is successful

  Scenario: Create a Boolean metric with empty bytes
    Given the type "Boolean"
    And the name "bool.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the bytes empty
    When the metric is created with bytes
    Then a conversion exception are thrown

  #Double metric tests

  Scenario: Create a Double metric with valid arguments
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "99.99"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Double metric with null name
    Given the type "Double"
    And the name null
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "99.99"
    When the metric is created
    Then an illegal argument exception are thrown

  Scenario: Create a Double metric with empty name
    Given the type "Double"
    And the name ""
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "99.99"
    When the metric is created
    Then an illegal argument exception are thrown

  Scenario: Create a Double metric with null tags
    Given the type "Double"
    And the name "double.metric"
    And the tags null
    And the timestamp 1496681817319
    And the value "99.99"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Double metric with empty tags
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      |  |  |
    And the timestamp 1496681817319
    And the value "99.99"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Double metric with null timestamp
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp null
    And the value "99.99"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Double metric with null value
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value null
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Double metric with null bytes
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the bytes null
    When the metric is created with bytes
    Then the metric creation is successful

  Scenario: Create a Double metric with empty bytes
    Given the type "Double"
    And the name "double.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the bytes empty
    When the metric is created with bytes
    Then a conversion exception are thrown

  #Long metric tests

  Scenario: Create a Long metric with valid arguments
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "99"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Long metric with null name
    Given the type "Long"
    And the name null
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "99"
    When the metric is created
    Then an illegal argument exception are thrown

  Scenario: Create a Long metric with empty name
    Given the type "Long"
    And the name ""
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "99"
    When the metric is created
    Then an illegal argument exception are thrown

  Scenario: Create a Long metric with null tags
    Given the type "Long"
    And the name "long.metric"
    And the tags null
    And the timestamp 1496681817319
    And the value "99"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Long metric with empty tags
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      |  |  |
    And the timestamp 1496681817319
    And the value "99"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Long metric with null timestamp
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp null
    And the value "99"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Long metric with null value
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value null
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a Long metric with null bytes
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the bytes null
    When the metric is created with bytes
    Then the metric creation is successful

  Scenario: Create a Long metric with empty bytes
    Given the type "Long"
    And the name "long.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the bytes empty
    When the metric is created with bytes
    Then a conversion exception are thrown

  #String metric tests

  Scenario: Create a String metric with valid arguments
    Given the type "String"
    And the name "string.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "text"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a String metric with null name
    Given the type "String"
    And the name null
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "text"
    When the metric is created
    Then an illegal argument exception are thrown

  Scenario: Create a String metric with empty name
    Given the type "String"
    And the name ""
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value "text"
    When the metric is created
    Then an illegal argument exception are thrown

  Scenario: Create a String metric with null tags
    Given the type "String"
    And the name "string.metric"
    And the tags null
    And the timestamp 1496681817319
    And the value "text"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a String metric with empty tags
    Given the type "String"
    And the name "string.metric"
    And the tags:
      |  |  |
    And the timestamp 1496681817319
    And the value "text"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a String metric with null timestamp
    Given the type "String"
    And the name "string.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp null
    And the value "text"
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a String metric with null value
    Given the type "String"
    And the name "string.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the value null
    When the metric is created
    Then the metric creation is successful

  Scenario: Create a String metric with null bytes
    Given the type "String"
    And the name "string.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the bytes null
    When the metric is created with bytes
    Then the metric creation is successful

  Scenario: Create a String metric with empty bytes
    Given the type "String"
    And the name "string.metric"
    And the tags:
      | name1 | value1 |
      | name2 | value2 |
    And the timestamp 1496681817319
    And the bytes empty
    When the metric is created with bytes
    Then the metric creation is successful
