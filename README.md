# fluent-plugin-dynamodb-alt

Fluent plugin to output to DynamoDB.

## Installation

```sh
bundle install
bundle exec rake install
```

## Configuration

```
<match tag>
  type dynamodb_alt
  aws_key_id AKIAIOSFODNN7EXAMPLE
  aws_sec_key  wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  region ap-northeast-1
  table_name my_table
  #timestamp_key time
  #concurrency 1

  # see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_RequestSyntax
  #expected id NULL,time LT ${time}
  #conditional_operator OR

  #include_time_key true
  #include_tag_key true
</match>
```
