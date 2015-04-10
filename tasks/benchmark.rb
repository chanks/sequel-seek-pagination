task :benchmark do
  require 'sequel-seek-pagination'

  Sequel::Database.extension :seek_pagination

  DB = Sequel.connect "postgres:///sequel-seek-pagination-test"

  DB.drop_table? :seek

  DB.create_table :seek do
    primary_key :id

    text :content, null: false # Prevent index-only scans.
  end

  RECORD_COUNT = 100000
  ITERATION_COUNT = 10

  DB[:seek].insert([:content], DB[Sequel.function(:generate_series, 1, RECORD_COUNT)].select{md5(Sequel.cast(random{}, :text))})

  puts
  puts "Single column, unique, not-null, ascending:"
  puts DB[:seek].order(:id).seek_paginate(30, after: rand(RECORD_COUNT) + 1).explain(analyze: true)

  puts
  puts "Single column, unique, not-null, descending:"
  puts DB[:seek].order(Sequel.desc(:id)).seek_paginate(30, after: rand(RECORD_COUNT) + 1).explain(analyze: true)

  puts
end
