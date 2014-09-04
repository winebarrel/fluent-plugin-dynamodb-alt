# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = 'fluent-plugin-dynamodb-alt'
  spec.version       = '0.1.2'
  spec.authors       = ['Genki Sugawara']
  spec.email         = ['sgwr_dts@yahoo.co.jp']
  spec.summary       = %q{Fluent plugin to output to DynamoDB.}
  spec.description   = %q{Fluent plugin to output to DynamoDB.}
  spec.homepage      = 'https://github.com/winebarrel/fluent-plugin-dynamodb-alt'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_dependency 'fluentd'
  spec.add_dependency 'aws-sdk-core', '>= 2.0.0.rc15'
  spec.add_dependency 'parallel'
  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '>= 3.0.0'
  spec.add_development_dependency 'hashie'
  spec.add_development_dependency 'ddbcli'
  spec.add_development_dependency 'msgpack'
end
