require 'fluent/test'
require 'fluent/plugin/out_dynamodb_alt'

# Disable Test::Unit
module Test::Unit::RunCount; def run(*); end; end

RSpec.configure do |config|
  config.before(:all) do
    Fluent::Test.setup
  end
end
