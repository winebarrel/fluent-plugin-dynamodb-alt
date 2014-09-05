describe Fluent::DynamodbAltOutput do
  let(:time) {
    Time.parse('2014-09-01 01:23:45 UTC').to_i
  }

  context('configure') {
    it do
      driver = create_driver
      expect(driver.instance).to receive(:configure_aws).with(
        :access_key_id     => "AKIAIOSFODNN7EXAMPLE",
        :secret_access_key => "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        :region            => "ap-northeast-1",
        :endpoint          => "http:://localhost:4567")

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
        endpoint http:://localhost:4567
        table_name my_table
        timestamp_key timestamp
        binary_keys aaa,bbb
        delete_key delete
        concurrency 2
        conditional_operator OR
      EOS

      expect(driver.instance.aws_key_id          ).to eq 'AKIAIOSFODNN7EXAMPLE'
      expect(driver.instance.aws_sec_key         ).to eq 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
      expect(driver.instance.region              ).to eq 'ap-northeast-1'
      expect(driver.instance.endpoint            ).to eq 'http:://localhost:4567'
      expect(driver.instance.table_name          ).to eq 'my_table'
      expect(driver.instance.timestamp_key       ).to eq 'timestamp'
      expect(driver.instance.binary_keys         ).to eq ['aaa', 'bbb']
      expect(driver.instance.delete_key          ).to eq 'delete'
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
        timestamp_key timestamp
      EOS

      expect(driver.instance.binary_keys).to eq []
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
        timestamp_key timestamp
        expected timestamp GE 0,key LT 100
      EOS

      expected = driver.instance.expected
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
        timestamp_key timestamp
        expected id NULL,timestamp LT ${ts},key EQ ${k}
      EOS

      expected = driver.instance.expected

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

      expect {
        driver.configure(<<-EOS)
          type dynamodb_alt
          table_name my_table
          timestamp_key timestamp
          expected key EQ val
        EOS
      }.to raise_error("Cannot parse the expected expression (key EQ val): 399: unexpected token at 'val]'")
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
        timestamp_key timestamp
        expected key1 EQ "str",key2 EQ 1
      EOS

      expected = driver.instance.expected
      expect(expected).to eq [["key1", "EQ", "str"], ["key2", "EQ", 1]]
    end
  }

  context('emit') {
    before(:all) do
      drop_table
      create_table('(id STRING HASH) READ = 20 WRITE = 20')
    end

    before(:each) do
      truncate_table
    end

    after(:all) do
      drop_table
    end

    context('without condition') {
      it do
        run_driver do |d|
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]
      end
    }

    context('concurrency > 1') {
      it do
        run_driver(:concurrency => 2) do |d|
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]
      end
    }

    context('with condition (1)') {
      it do
        run_driver(:expected => 'id NULL,timestamp LT ${timestamp}', :conditional_operator => 'OR') do |d|
          expect(d.instance.log).not_to receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]

        run_driver(:expected => 'id NULL,timestamp LT ${timestamp}', :conditional_operator => 'OR') do |d|
          expect(d.instance.log).not_to receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625004, 'key' => 'val'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625005, 'key' => 'val'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625006, 'key' => 'val'}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625004, 'key' => 'val'},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625005, 'key' => 'val'},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625006, 'key' => 'val'},
        ]
      end

      it do
        run_driver(:expected => 'id NULL,timestamp LT ${timestamp}', :conditional_operator => 'OR') do |d|
          expect(d.instance.log).not_to receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]

        run_driver(:expected => 'id NULL,timestamp LT ${timestamp}', :conditional_operator => 'OR') do |d|
          expect(d.instance.log).to receive(:warn)
            .with(%!The conditional request failed: {:table_name=>"#{TEST_TABLE_NAME}", :item=>{"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001, "key"=>"val"}, :expected=>{"id"=>{:comparison_operator=>"NULL"}, "timestamp"=>{:comparison_operator=>"LT", :attribute_value_list=>[1409534625001]}}, :conditional_operator=>"OR"}!)
          expect(d.instance.log).to receive(:warn)
            .with(%!The conditional request failed: {:table_name=>"#{TEST_TABLE_NAME}", :item=>{"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002, "key"=>"val"}, :expected=>{"id"=>{:comparison_operator=>"NULL"}, "timestamp"=>{:comparison_operator=>"LT", :attribute_value_list=>[1409534625002]}}, :conditional_operator=>"OR"}!)
          expect(d.instance.log).to receive(:warn)
            .with(%!The conditional request failed: {:table_name=>"#{TEST_TABLE_NAME}", :item=>{"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003, "key"=>"val"}, :expected=>{"id"=>{:comparison_operator=>"NULL"}, "timestamp"=>{:comparison_operator=>"LT", :attribute_value_list=>[1409534625003]}}, :conditional_operator=>"OR"}!)

          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001, 'key' => 'val'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002, 'key' => 'val'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003, 'key' => 'val'}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]
      end
    }

    context('with condition (2)') {
      it do
        run_driver do |d|
          expect(d.instance.log).not_to receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001, 'key' => 'val'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002, 'key' => 'val'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003, 'key' => 'val'}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001, 'key' => 'val'},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002, 'key' => 'val'},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003, 'key' => 'val'},
        ]

        run_driver(:expected => 'timestamp LE ${timestamp},key EQ "val"') do |d|
          expect(d.instance.log).not_to receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625004, 'key' => 'val2'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625005, 'key' => 'val2'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625006, 'key' => 'val2'}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625004, 'key' => 'val2'},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625005, 'key' => 'val2'},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625006, 'key' => 'val2'},
        ]
      end

      it do
        run_driver do |d|
          expect(d.instance.log).not_to receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001, 'key' => 'val3'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002, 'key' => 'val3'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003, 'key' => 'val3'}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001, 'key' => 'val3'},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002, 'key' => 'val3'},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003, 'key' => 'val3'},
        ]

        run_driver(:expected => 'timestamp LE ${timestamp},key EQ "val"') do |d|
          expect(d.instance.log).to receive(:warn)
            .with(%!The conditional request failed: {:table_name=>"#{TEST_TABLE_NAME}", :item=>{"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625004, "key"=>"val2"}, :expected=>{"timestamp"=>{:comparison_operator=>"LE", :attribute_value_list=>[1409534625004]}, "key"=>{:comparison_operator=>"EQ", :attribute_value_list=>["val"]}}, :conditional_operator=>"AND"}!)
          expect(d.instance.log).to receive(:warn)
            .with(%!The conditional request failed: {:table_name=>"#{TEST_TABLE_NAME}", :item=>{"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625005, "key"=>"val2"}, :expected=>{"timestamp"=>{:comparison_operator=>"LE", :attribute_value_list=>[1409534625005]}, "key"=>{:comparison_operator=>"EQ", :attribute_value_list=>["val"]}}, :conditional_operator=>"AND"}!)
          expect(d.instance.log).to receive(:warn)
            .with(%!The conditional request failed: {:table_name=>"#{TEST_TABLE_NAME}", :item=>{"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625006, "key"=>"val2"}, :expected=>{"timestamp"=>{:comparison_operator=>"LE", :attribute_value_list=>[1409534625006]}, "key"=>{:comparison_operator=>"EQ", :attribute_value_list=>["val"]}}, :conditional_operator=>"AND"}!)

          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625004, 'key' => 'val2'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625005, 'key' => 'val2'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625006, 'key' => 'val2'}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001, 'key' => 'val3'},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002, 'key' => 'val3'},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003, 'key' => 'val3'},
        ]
      end

      it do
        run_driver do |d|
          expect(d.instance.log).not_to receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001, 'key' => 'val3'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002, 'key' => 'val3'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003, 'key' => 'val3'}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001, 'key' => 'val3'},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002, 'key' => 'val3'},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003, 'key' => 'val3'},
        ]

        run_driver(:expected => 'timestamp LE ${timestamp},key EQ "val"', :conditional_operator => 'OR') do |d|
          expect(d.instance.log).to_not receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625004, 'key' => 'val2'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625005, 'key' => 'val2'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625006, 'key' => 'val2'}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625004, 'key' => 'val2'},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625005, 'key' => 'val2'},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625006, 'key' => 'val2'},
        ]
      end
    }

    context('key dose not exist') {
      it do
        run_driver do |d|
          expect(d.instance.log).to receive(:warn)
            .with(%!Hash Key 'id' does not exist in the record: {"timestamp"=>1409534625001, "key"=>"val"}!)
          d.emit({'timestamp' => 1409534625001, 'key' => 'val'}, time)
        end

        expect(select_all).to match_array []
      end

      it do
        run_driver do |d|
          expect(d.instance.log).to receive(:warn)
            .with(%!Timestamp Key 'timestamp' does not exist in the record: {"id"=>"12345678-1234-1234-1234-123456789001", "key"=>"val"}!)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'key' => 'val'}, time)
        end

        expect(select_all).to match_array []
      end
    }

    context('aggregate') {
      it do
        run_driver(:flush_interval => 3) do |d|
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625003}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001}, time)

          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625003}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625001}, time)

          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625001}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625003},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625003},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]
      end

      it do
        run_driver(:flush_interval => 3) do |d|
          expect(d.instance).to receive(:put_record).with('id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625003)
          expect(d.instance).to receive(:put_record).with('id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625003)
          expect(d.instance).to receive(:put_record).with('id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003)

          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625003}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001}, time)

          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625003}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625001}, time)

          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625001}, time)
        end
      end
    }

    context('binary') {
      it do
        run_driver(:binary_keys => 'data') do |d|
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001, 'data' => MessagePack.pack({'foo' => 100, 'bar' => 'Zzz...'})}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002, 'data' => MessagePack.pack({'foo' => 200, 'bar' => 'Zzz..'})}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003, 'data' => MessagePack.pack({'foo' => 300, 'bar' => 'Zzz.'})}, time)
        end

        rows = select_all.map do |row|
          data = row['data']
          data = Base64.strict_decode64(data)
          row['data'] = MessagePack.unpack(data)
          row
        end

        expect(rows).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001, "data" => {'foo' => 100, 'bar' => 'Zzz...'}},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002, "data" => {'foo' => 200, 'bar' => 'Zzz..'}},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003, "data" => {'foo' => 300, 'bar' => 'Zzz.'}},
        ]
      end
    }

    context('set') {
      it do
        run_driver do |d|
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001, 'set' => [1, 2, 3]}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002, 'set' => ['1', '2', '3']}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003, 'set' => ['A', 'B', 'C']}, time)
        end

        rows = select_all.map do |row|
          row['set'].sort!
          row
        end

        expect(rows).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001, "set"=>[1, 2, 3]},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002, "set"=>['1', '2', '3']},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003, "set"=>['A', 'B', 'C']},
        ]
      end
    }

    context('delete record (1)') {
      it do
        run_driver do |d|
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]

        run_driver(:expected => 'id NULL,timestamp LT ${timestamp}', :conditional_operator => 'OR', :delete_key => 'delete') do |d|
          expect(d.instance.log).not_to receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625004, 'delete' => true}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625005, 'key' => 'val'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625006, 'delete' => true}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625005, 'key' => 'val'},
        ]
      end
    }

    context('delete record (2)') {
      it do
        run_driver do |d|
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]

        run_driver(:expected => 'id NULL,timestamp LT ${timestamp}', :conditional_operator => 'OR', :delete_key => 'delete') do |d|
          expect(d.instance.log).to receive(:warn)
            .with(%!The conditional request failed: {:table_name=>"#{TEST_TABLE_NAME}", :key=>{"id"=>"12345678-1234-1234-1234-123456789001"}, :expected=>{"id"=>{:comparison_operator=>"NULL"}, "timestamp"=>{:comparison_operator=>"LT", :attribute_value_list=>[1409534625001]}}, :conditional_operator=>"OR"}!)
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001, 'delete' => true}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625005, 'key' => 'val'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625006, 'delete' => true}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625005, 'key' => 'val'},
        ]
      end
    }

    context('delete empty record') {
      it do
        run_driver do |d|
          d.emit({'id' => '12345678-1234-1234-1234-123456789001', 'timestamp' => 1409534625001}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625002}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789003', 'timestamp' => 1409534625003}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625002},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]

        run_driver(:expected => 'id NULL,timestamp LT ${timestamp}', :conditional_operator => 'OR', :delete_key => 'delete') do |d|
          expect(d.instance.log).not_to receive(:warn)
          d.emit({'id' => '12345678-1234-1234-1234-123456789011', 'timestamp' => 1409534625004, 'delete' => true}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789002', 'timestamp' => 1409534625005, 'key' => 'val'}, time)
          d.emit({'id' => '12345678-1234-1234-1234-123456789013', 'timestamp' => 1409534625006, 'delete' => true}, time)
        end

        expect(select_all).to match_array [
          {"id"=>"12345678-1234-1234-1234-123456789001", "timestamp"=>1409534625001},
          {"id"=>"12345678-1234-1234-1234-123456789002", "timestamp"=>1409534625005, 'key' => 'val'},
          {"id"=>"12345678-1234-1234-1234-123456789003", "timestamp"=>1409534625003},
        ]
      end
    }
  }
end
