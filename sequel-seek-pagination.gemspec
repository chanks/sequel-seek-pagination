# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sequel/seek_pagination/version'

Gem::Specification.new do |spec|
  spec.name          = 'sequel-seek-pagination'
  spec.version       = Sequel::SeekPagination::VERSION
  spec.authors       = ["Chris Hanks"]
  spec.email         = ['christopher.m.hanks@gmail.com']
  spec.summary       = %q{Seek pagination for Sequel + PostgreSQL}
  spec.description   = %q{Generic, flexible seek pagination implementation for Sequel and PostgreSQL}
  spec.homepage      = 'https://github.com/chanks/sequel-seek-pagination'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.6'
  spec.add_development_dependency 'rake'

  spec.add_dependency 'sequel', '~> 4.0'
end
