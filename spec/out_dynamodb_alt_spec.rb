describe Fluent::DynamodbAltOutput do
  context 'configure' do
    it do
      driver = create_driver
      expect(driver.instance).to receive(:configure_aws).with(
        :access_key_id     => "AKIAIOSFODNN7EXAMPLE",
        :secret_access_key => "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        :region            => "ap-northeast-1")

      expect(driver.instance).to receive(:create_client) {
        client = double('client')
        allow(client).to receive(:describe_table).with(:table_name => 'my_table') {
          Hashie::Mash.new(:table => {
            :key_schema => [
              {:key_type => 'HASH',  :attribute_name => 'hash_key'},
              {:key_type => 'RANGE', :attribute_name => 'range_key'}
            ]})
        }
        client
      }

      driver.configure(<<-EOS)
        type dynamodb_alt
        aws_key_id AKIAIOSFODNN7EXAMPLE
        aws_sec_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
        region ap-northeast-1
        table_name my_table
        concurrency 2
        conditional_operator OR
      EOS

      expect(driver.instance.aws_key_id          ).to eq 'AKIAIOSFODNN7EXAMPLE'
      expect(driver.instance.aws_sec_key         ).to eq 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
      expect(driver.instance.table_name          ).to eq 'my_table'
      expect(driver.instance.concurrency         ).to eq 2
      expect(driver.instance.conditional_operator).to eq 'OR'
      expect(driver.instance.instance_variable_get(:@hash_key) ).to eq 'hash_key'
      expect(driver.instance.instance_variable_get(:@range_key)).to eq 'range_key'
    end

    it do
      driver = create_driver
      allow(driver.instance).to receive(:configure_aws)

      allow(driver.instance).to receive(:create_client) {
        client = double('client')
        allow(client).to receive(:describe_table) {
          Hashie::Mash.new(:table => {
            :key_schema => [
              {:key_type => 'HASH',  :attribute_name => 'hash_key'},
            ]})
        }
        client
      }

      driver.configure(<<-EOS)
        type dynamodb_alt
        table_name my_table
        expected timestamp GE 0,key LT 100
      EOS

      expected = driver.instance.instance_variable_get(:@expected)
      expect(expected).to eq [["timestamp", "GE", 0],["key", "LT", 100]]
    end

    it do
      driver = create_driver
      allow(driver.instance).to receive(:configure_aws)

      allow(driver.instance).to receive(:create_client) {
        client = double('client')
        allow(client).to receive(:describe_table) {
          Hashie::Mash.new(:table => {
            :key_schema => [
              {:key_type => 'HASH',  :attribute_name => 'hash_key'},
            ]})
        }
        client
      }

      driver.configure(<<-EOS)
        type dynamodb_alt
        table_name my_table
        expected id NULL,timestamp LT ${ts},key EQ ${k}
      EOS

      expected = driver.instance.instance_variable_get(:@expected)

      expect(expected[0]).to eq ["id", "NULL", nil]

      col1, op1, val1 = expected[1]
      expect(col1).to eq 'timestamp'
      expect(op1 ).to eq 'LT'
      expect(val1.call('ts' => 1)).to eq 1

      col2, op2, val2 = expected[2]
      expect(col2).to eq 'key'
      expect(op2 ).to eq 'EQ'
      expect(val2.call('k' => '1')).to eq '1'
    end
  end
end
