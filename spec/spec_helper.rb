require 'sequel-seek-pagination'

Sequel::Database.extension :seek_pagination

DB = Sequel.connect "postgres:///sequel-seek-pagination-test"

DB.drop_table? :seek

DB.create_table :seek do
  primary_key :id

  text :content, null: false # Prevent index-only scans.
end

DB[:seek].insert([:content], DB[Sequel.function(:generate_series, 1, 1000)].select{md5(Sequel.cast(random{}, :text))})
