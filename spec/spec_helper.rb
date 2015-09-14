require 'sequel'

$: << File.join(File.dirname(__FILE__), '..', 'lib')

Sequel.extension :core_extensions

Sequel::Database.extension :seek_pagination

DB = Sequel.connect "postgres:///sequel-seek-pagination-test"

DB.drop_table? :seek

DB.create_table :seek do
  primary_key :id

  integer :not_nullable_1, null: false
  integer :not_nullable_2, null: false

  integer :nullable_1
  integer :nullable_2
end

DB.run <<-SQL
  INSERT INTO seek
    (not_nullable_1, not_nullable_2, nullable_1, nullable_2)
  SELECT trunc(random() * 10 + 1),
         trunc(random() * 10 + 1),
         CASE WHEN random() > 0.5 THEN trunc(random() * 10 + 1) ELSE NULL END,
         CASE WHEN random() > 0.5 THEN trunc(random() * 10 + 1) ELSE NULL END
  FROM generate_series(1, 100) s
SQL

require 'pry'
require 'minitest/autorun'
require 'minitest/rg'
