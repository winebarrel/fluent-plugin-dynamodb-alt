# fluent-plugin-dynamodb-alt

Alternative fluent plugin to output to DynamoDB.

[![Gem Version](https://badge.fury.io/rb/fluent-plugin-dynamodb-alt.png)](http://badge.fury.io/rb/fluent-plugin-dynamodb-alt)
[![Build Status](https://travis-ci.org/winebarrel/fluent-plugin-dynamodb-alt.svg)](https://travis-ci.org/winebarrel/fluent-plugin-dynamodb-alt)

## Features

* Use [PutItem](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html) Action.
* Sort the records in the timestamp key.
* Aggregate the records in the primary key.
* Support [Expected constraint](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-Expected).

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
  aws_sec_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  region ap-northeast-1
  table_name my_table
  timestamp_key timestamp
  #binary_keys data1,data2
  #endpoint http:://localhost:4567
  #concurrency 1
  #delete_key delete

  # see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-Expected
  #expected id NULL,timestamp LT ${timestamp},key EQ "val"
  #conditional_operator OR

  #include_time_key false
  #include_tag_key false
</match>
```
