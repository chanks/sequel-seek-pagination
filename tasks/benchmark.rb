task :benchmark do
  require 'sequel-seek-pagination'

  RECORD_COUNT = 100000
  ITERATION_COUNT = 10

  Sequel.extension :core_extensions
  Sequel::Database.extension :seek_pagination

  DB = Sequel.connect "postgres:///sequel-seek-pagination-test"

  DB.drop_table? :seek

  DB.create_table :seek do
    primary_key :id

    integer :non_nullable_1, null: false
    integer :non_nullable_2, null: false

    integer :nullable_1
    integer :nullable_2

    text :content # Prevent index-only scans.
  end

  DB.run <<-SQL
    INSERT INTO seek
      (non_nullable_1, non_nullable_2, nullable_1, nullable_2, content)
    SELECT trunc(random() * 10 + 1),
           trunc(random() * 10 + 1),
           CASE WHEN random() > 0.5 THEN trunc(random() * 10 + 1) ELSE NULL END,
           CASE WHEN random() > 0.5 THEN trunc(random() * 10 + 1) ELSE NULL END,
           md5(random()::text)
    FROM generate_series(1, #{RECORD_COUNT}) s
  SQL

  DB.add_index :seek, [:non_nullable_1]

  {
    "1 column, not-null, ascending, no not-null information"  => DB[:seek].order(:id.asc ).seek_paginate(30, after: rand(RECORD_COUNT) + 1),
    "1 column, not-null, descending, no not-null information" => DB[:seek].order(:id.desc).seek_paginate(30, after: rand(RECORD_COUNT) + 1),

    "1 column, not-null, ascending, with not-null information"  => DB[:seek].order(:id.asc ).seek_paginate(30, after: rand(RECORD_COUNT) + 1, not_null: [:id, :not_nullable_1, :not_nullable_2]),
    "1 column, not-null, descending, with not-null information" => DB[:seek].order(:id.desc).seek_paginate(30, after: rand(RECORD_COUNT) + 1, not_null: [:id, :not_nullable_1, :not_nullable_2]),

    "2 columns, not-null, ascending, no not-null information"  => DB[:seek].order(:non_nullable_1.asc,  :id.asc ).seek_paginate(30, after: [5, rand(RECORD_COUNT) + 1]),
    "2 columns, not-null, descending, no not-null information" => DB[:seek].order(:non_nullable_1.desc, :id.desc).seek_paginate(30, after: [5, rand(RECORD_COUNT) + 1]),

    "2 columns, not-null, ascending, with not-null information"  => DB[:seek].order(:non_nullable_1.asc,  :id.asc ).seek_paginate(30, after: [5, rand(RECORD_COUNT) + 1], not_null: [:id, :non_nullable_1, :non_nullable_2]),
    "2 columns, not-null, descending, with not-null information" => DB[:seek].order(:non_nullable_1.desc, :id.desc).seek_paginate(30, after: [5, rand(RECORD_COUNT) + 1], not_null: [:id, :non_nullable_1, :non_nullable_2]),
  }.each do |description, ds|
    puts
    puts description + ':'
    ds.explain(analyze: true) # Make sure everything is cached.
    puts ds.sql
    puts ds.explain(analyze: true)
    puts
  end
end
