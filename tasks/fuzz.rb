task :fuzz do
  RECORD_COUNT = 10000
  ITERATION_COUNT = 10
  TESTS_PER_ITERATION = 50

  require 'sequel-seek-pagination'

  Sequel::Database.extension :seek_pagination

  DB = Sequel.connect "postgres:///sequel-seek-pagination-test"

  DB.drop_table? :seek

  DB.create_table :seek do
    primary_key :pk
    integer :non_nullable_1, null: false
    integer :non_nullable_2, null: false
    integer :nullable_1
    integer :nullable_2
  end

  def random(max)
    rand(max) + 1
  end

  def repopulate_seek(count = RECORD_COUNT)
    DB.run <<-SQL
      INSERT INTO seek
        (non_nullable_1, non_nullable_2, nullable_1, nullable_2)
      SELECT trunc(random() * 10 + 1),
             trunc(random() * 10 + 1),
             CASE WHEN random() > 0.5 THEN trunc(random() * 10 + 1) ELSE NULL END,
             CASE WHEN random() > 0.5 THEN trunc(random() * 10 + 1) ELSE NULL END
      FROM generate_series(1, #{count}) s
    SQL
  end

  (1..ITERATION_COUNT).each do |i|
    puts "Iteration #{i}..."

    DB[:seek].delete
    repopulate_seek

    TESTS_PER_ITERATION.times do
      # Will add nullable columns when those are better supported.
      possible_columns = [:non_nullable_1, :non_nullable_2]
      columns = possible_columns.sample(random(possible_columns.count))

      all_columns = columns + [:pk]

      offset = random(RECORD_COUNT - 1)

      ordering = all_columns.map do |column|
        rand < 0.5 ? Sequel.asc(column) : Sequel.desc(column)
      end

      after = DB[:seek].order(*ordering).offset(offset).get(all_columns)

      expected = DB[:seek].order(*ordering).offset(offset + 1).limit(10).all
      actual   = DB[:seek].order(*ordering).seek_paginate(10, after: after).all

      raise "Bad!" unless actual == expected
    end
  end

end
