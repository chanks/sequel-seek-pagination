task :benchmark do
  require 'sequel-seek-pagination'

  Sequel::Database.extension :seek_pagination

  DB = Sequel.connect "postgres:///sequel-seek-pagination-test"

  DB.drop_table? :seek

  DB.create_table :seek do
    primary_key :id

    integer :col1, null: false

    text :content, null: false # Prevent index-only scans.

    index [:col1, :id]
  end

  RECORD_COUNT = 100000
  ITERATION_COUNT = 10

  DB[:seek].insert([:content, :col1], DB[Sequel.function(:generate_series, 1, RECORD_COUNT).as(:i)].select{[md5(Sequel.cast(random{}, :text)), mod(:i, 100) + 1]})

  puts
  puts "Single column, unique, not-null, ascending:"
  puts DB[:seek].order(:id).seek_paginate(30, after: rand(RECORD_COUNT) + 1).explain(analyze: true)

  puts
  puts "Single column, unique, not-null, descending:"
  puts DB[:seek].order(Sequel.desc(:id)).seek_paginate(30, after: rand(RECORD_COUNT) + 1).explain(analyze: true)

  puts
  puts "Multiple columns, unique, not-null, ascending:"
  puts DB[:seek].order(:col1, :id).seek_paginate(30, after: [5, rand(RECORD_COUNT) + 1]).explain(analyze: true)

  puts
  puts "Multiple columns, unique, not-null, descending:"
  puts DB[:seek].order(Sequel.desc(:col1), Sequel.desc(:id)).seek_paginate(30, after: [5, rand(RECORD_COUNT) + 1]).explain(analyze: true)

  puts
end
