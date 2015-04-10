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

  {
    "Single column, unique, not-null, ascending"     => DB[:seek].order(:id).seek_paginate(30, after: rand(RECORD_COUNT) + 1),
    "Single column, unique, not-null, descending"    => DB[:seek].order(Sequel.desc(:id)).seek_paginate(30, after: rand(RECORD_COUNT) + 1),
    "Multiple columns, unique, not-null, ascending"  => DB[:seek].order(:col1, :id).seek_paginate(30, after: [5, rand(RECORD_COUNT) + 1]),
    "Multiple columns, unique, not-null, descending" => DB[:seek].order(Sequel.desc(:col1), Sequel.desc(:id)).seek_paginate(30, after: [5, rand(RECORD_COUNT) + 1]),
  }.each do |description, ds|
    puts
    puts description + ':'
    ds.explain(analyze: true) # Make sure everything is cached.
    puts ds.explain(analyze: true)
    puts
  end
end
