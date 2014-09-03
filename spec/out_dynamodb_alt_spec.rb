describe Fluent::DynamodbAltOutput do
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
    EOS

    expect(driver.instance.aws_key_id ).to eq 'AKIAIOSFODNN7EXAMPLE'
    expect(driver.instance.aws_sec_key).to eq 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    expect(driver.instance.table_name ).to eq 'my_table'
    expect(driver.instance.instance_variable_get(:@hash_key) ).to eq 'hash_key'
    expect(driver.instance.instance_variable_get(:@range_key)).to eq 'range_key'
  end
end
