require 'sequel-seek-pagination'

Sequel::Database.extension :seek_pagination

DB = Sequel.connect "postgres:///sequel-seek-pagination-test"

DB.drop_table? :seek

DB.create_table :seek do
  integer :id, primary_key: true
end

DB[:seek].import([:id], DB[Sequel.function(:generate_series, 1, 10000)])
