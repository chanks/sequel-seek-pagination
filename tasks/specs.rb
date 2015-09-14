require 'rake'
require 'rake/testtask'

Rake::TestTask.new :default do |t|
  t.libs = ['spec']
  t.pattern = 'spec/**/*_spec.rb'
end

task :spec => :default
task :test => :default
