require 'fluent/test'
require 'fluent/plugin/out_dynamodb_alt'
require 'hashie'

DRIVER_DEFAULT_TAG = 'test.default'

# Disable Test::Unit
module Test::Unit::RunCount; def run(*); end; end

RSpec.configure do |config|
  config.before(:all) do
    Fluent::Test.setup
  end
end

def create_driver(tag = DRIVER_DEFAULT_TAG)
  Fluent::Test::OutputTestDriver.new(Fluent::DynamodbAltOutput, tag)
end

def run_driver(options = {})
  options = options.dup

  additional_options = options.map {|key, value|
    "#{key} #{value}"
  }.join("\n")

  fluentd_conf = <<-EOS
    type dynamodb_alt
    aws_key_id AKIAIOSFODNN7EXAMPLE
    aws_sec_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    region ap-northeast-1
    table_name my_table
    #{additional_options}
  EOS

  tag = options[:tag] || DRIVER_DEFAULT_TAG
  driver = create_driver(tag).configure(fluentd_conf)

  driver.run do
    yield(driver)
  end
end
