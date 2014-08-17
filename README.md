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
  aws_key_id ...
  aws_sec_key ...
  region ap-northeast-1
  table_name any_table_nam

  # see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_RequestSyntax
  #expected id NULL,time LT ${time}
  #conditional_operator OR

  #include_time_key true
  #include_tag_key true
</match>
```
