task :fuzz do
  require 'pp'

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
      possible_columns = [
                           :non_nullable_1,
                           :non_nullable_2,
                           :nullable_1,
                           :nullable_2,
                         ]

      columns = possible_columns.sample(random(possible_columns.count))

      all_columns = columns + [:pk]

      offset = random(RECORD_COUNT - 1)

      ordering = all_columns.map do |column|
        direction = rand < 0.5 ? :asc : :desc
        nulls = rand < 0.5 ? :first : :last
        Sequel.send(direction, column, nulls: nulls)
      end

      ds = DB[:seek].order(*ordering)

      after    = ds.offset(offset).get(all_columns)
      expected = ds.offset(offset + 1).limit(10).all
      actual   = ds.seek_paginate(10, after: after).all

      unless actual == expected
        puts "Uh-oh!"
        puts "ds = #{ds.sql}"
        puts "after = #{after.inspect}"
        puts
        puts "Expected:"
        pp expected.map{|h| h.values_at(*all_columns)}
        puts
        puts "Actual:"
        pp actual.map{|h| h.values_at(*all_columns)}

        $break = true
        ds.seek_paginate(10, after: after)

        exit
      end
    end
  end

end
