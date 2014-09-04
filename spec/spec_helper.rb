require 'fluent/test'
require 'fluent/plugin/out_dynamodb_alt'
require 'base64'
require 'hashie'
require 'msgpack'
require 'securerandom'
require 'stringio'

DRIVER_DEFAULT_TAG = 'test.default'
TEST_TABLE_NAME = 'my_table-' + SecureRandom.uuid
TEST_AWS_KEY_ID = ENV['OUT_DYNAMODB_ALT_SPEC_AWS_KEY_ID'] || 'scott'
TEST_AWS_SEC_KEY = ENV['OUT_DYNAMODB_ALT_SPEC_AWS_SEC_KEY'] || 'tiger'
TEST_REGION = ENV['OUT_DYNAMODB_ALT_SPEC_REGION'] || 'us-west-1'

# Disable Test::Unit
module Test::Unit::RunCount; def run(*); end; end

RSpec.configure do |config|
  config.before(:all) do
    Fluent::Test.setup
  end
end

def query(sql)
  out = `ddbcli -k '#{TEST_AWS_KEY_ID}' -s '#{TEST_AWS_SEC_KEY}' -r #{TEST_REGION} -e "#{sql.gsub(/\n/, ' ')}"`
  raise out unless $?.success?
  return out
end

def select_all
  JSON.parse(query("SELECT ALL * FROM #{TEST_TABLE_NAME}"))
end

def create_driver(tag = DRIVER_DEFAULT_TAG)
  Fluent::Test::OutputTestDriver.new(Fluent::DynamodbAltOutput, tag)
end

def run_driver(options = {})
  options = {
    :flush_interval => 0
  }.merge(options.dup)

  additional_options = options.map {|key, value|
    "#{key} #{value}"
  }.join("\n")

  fluentd_conf = <<-EOS
    type dynamodb_alt
    aws_key_id #{TEST_AWS_KEY_ID}
    aws_sec_key #{TEST_AWS_SEC_KEY}
    region #{TEST_REGION}
    table_name #{TEST_TABLE_NAME}
    timestamp_key timestamp
    #{additional_options}
  EOS

  tag = options[:tag] || DRIVER_DEFAULT_TAG
  driver = create_driver(tag)

  driver.configure(fluentd_conf)

  driver.run do
    yield(driver)
  end
end

def create_table(attrs)
  query("CREATE TABLE #{TEST_TABLE_NAME} #{attrs}")

  loop do
    status = query("SHOW TABLE STATUS LIKE '#{TEST_TABLE_NAME}'")
    status = JSON.parse(status)
    break if (not status[TEST_TABLE_NAME] or status[TEST_TABLE_NAME]['TableStatus'] == 'ACTIVE')
  end
end

def drop_table
  table_is_exist = proc do
    tables = query("SHOW TABLES LIKE '#{TEST_TABLE_NAME}'")
    tables = JSON.parse(tables)
    not tables.empty?
  end

  return unless table_is_exist.call

  query("DROP TABLE #{TEST_TABLE_NAME}")

  loop do
    tables = query("SHOW TABLES LIKE '#{TEST_TABLE_NAME}'")
    tables = JSON.parse(tables)
    break unless table_is_exist.call
  end
end

def truncate_table
  query("DELETE ALL FROM #{TEST_TABLE_NAME}")

  loop do
    count = query("SELECT ALL COUNT(*) FROM #{TEST_TABLE_NAME}").strip
    break if count == '0'
  end
end
