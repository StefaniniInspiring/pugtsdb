Feature: Pug instantiation tests

  Scenario: Create a Pug instance with valid arguments
    When create a Pug instance with storage path "/tmp/pugtest" user "test" and pass "test"
    Then the Pug instance is created successful

  Scenario: Create a Pug instance with null storage path
    When create a Pug instance with storage path null user "test" and pass "test"
    Then an illegal argument exception are thrown

  Scenario: Create a Pug instance with empty storage path
    When create a Pug instance with storage path "" user "test" and pass "test"
    Then an illegal argument exception are thrown

  Scenario: Create a Pug instance with null username
    When create a Pug instance with storage path "/tmp/pugtest" user null and pass "test"
    Then an illegal argument exception are thrown

  Scenario: Create a Pug instance with empty username
    When create a Pug instance with storage path "/tmp/pugtest" user "" and pass "test"
    Then an illegal argument exception are thrown

  Scenario: Create a Pug instance with null password
    When create a Pug instance with storage path "/tmp/pugtest" user "test" and pass null
    Then an illegal argument exception are thrown

  Scenario: Create a Pug instance with empty password
    When create a Pug instance with storage path "/tmp/pugtest" user "test" and pass ""
    Then an illegal argument exception are thrown