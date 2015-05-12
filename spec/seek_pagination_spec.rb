require 'spec_helper'

describe Sequel::SeekPagination do
  before do
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

    rows = []

    [true, false].each do |nullable_1|
      [true, false].each do |nullable_2|
        250.times do
          row = {
            non_nullable_1: random(10),
            non_nullable_2: random(10)
          }

          row[:nullable_1] = (random(10) if nullable_1)
          row[:nullable_2] = (random(10) if nullable_2)

          rows << row
        end
      end
    end

    DB[:seek].multi_insert(rows.shuffle)
  end

  it "should raise an error if the dataset is not ordered" do
    proc {
      DB[:seek].seek_paginate(30)
    }.should raise_error Sequel::SeekPagination::Error, /cannot seek paginate on a dataset with no order/
  end

  describe "when ordering by a single column" do
    it "should limit the dataset appropriately when a starting point is not given" do
      DB[:seek].order(:pk).seek_paginate(5).all.should == DB[:seek].order_by(:pk).limit(5).all

      # Then in reverse:
      DB[:seek].order(Sequel.desc(:pk)).seek_paginate(5).all.should == DB[:seek].order_by(Sequel.desc(:pk)).limit(5).all
    end

    it "should page properly when given a starting point" do
      pk = DB[:seek].order(:pk).offset(56).get(:pk)

      result = DB[:seek].order(:pk).seek_paginate(5, after: pk).all
      result.should == DB[:seek].order(:pk).offset(57).limit(5).all

      result = DB[:seek].order(:pk).seek_paginate(5, after: [pk]).all
      result.should == DB[:seek].order(:pk).offset(57).limit(5).all

      # Then in reverse:
      pk = DB[:seek].order(Sequel.desc(:pk)).offset(56).get(:pk)

      result = DB[:seek].order(Sequel.desc(:pk)).seek_paginate(5, after: pk).all
      result.should == DB[:seek].order(Sequel.desc(:pk)).offset(57).limit(5).all

      result = DB[:seek].order(Sequel.desc(:pk)).seek_paginate(5, after: [pk]).all
      result.should == DB[:seek].order(Sequel.desc(:pk)).offset(57).limit(5).all
    end

    it "should accept whatever type of order clause is there" do
      datasets = [
        DB[:seek].order(:pk),
        DB[:seek].order(:seek__pk),
        DB[:seek].order(Sequel.asc(:pk)),
        DB[:seek].order(Sequel.asc(:seek__pk)),
        DB[:seek].order(Sequel.desc(:pk)).reverse_order
      ]

      datasets.each do |dataset|
        dataset.seek_paginate(5).all.should == DB[:seek].order_by(:pk).limit(5).all
      end

      # Then in reverse:
      DB[:seek].order(Sequel.desc(:pk)).seek_paginate(5).all.should == DB[:seek].order_by(Sequel.desc(:pk)).limit(5).all

      # With starting points:
      datasets.each do |dataset|
        pk = DB[:seek].order(:pk).offset(56).get(:pk)

        result = dataset.seek_paginate(5, after: pk).all
        result.should == DB[:seek].order(:pk).offset(57).limit(5).all

        result = dataset.seek_paginate(5, after: [pk]).all
        result.should == DB[:seek].order(:pk).offset(57).limit(5).all
      end

      # Then in reverse:
      pk = DB[:seek].order(Sequel.desc(:pk)).offset(56).get(:pk)

      result = DB[:seek].order(Sequel.desc(:pk)).seek_paginate(5, after: pk).all
      result.should == DB[:seek].order(Sequel.desc(:pk)).offset(57).limit(5).all

      result = DB[:seek].order(Sequel.desc(:pk)).seek_paginate(5, after: [pk]).all
      result.should == DB[:seek].order(Sequel.desc(:pk)).offset(57).limit(5).all
    end
  end

  describe "when ordering by multiple columns" do
    describe "by two columns" do
      it "should limit the dataset appropriately when a starting point is not given" do
        result = DB[:seek].order(:non_nullable_1, :pk).seek_paginate(5).all
        result.should == DB[:seek].order(:non_nullable_1, :pk).limit(5).all

        # Then in reverse:
        results = DB[:seek].order(Sequel.desc(:seek__non_nullable_1), Sequel.desc(:seek__pk)).seek_paginate(5).all
        results.should == DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:pk)).limit(5).all
      end

      it "should page properly when given a starting point" do
        pair = DB[:seek].order(:non_nullable_1, :pk).offset(56).get([:non_nullable_1, :pk])

        result = DB[:seek].order(:non_nullable_1, :pk).seek_paginate(5, after: pair).all
        result.should == DB[:seek].order(:non_nullable_1, :pk).offset(57).limit(5).all

        # Then in reverse:
        pair = DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:pk)).offset(56).get([:non_nullable_1, :pk])

        result = DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:pk)).seek_paginate(5, after: pair).all
        result.should == DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:pk)).offset(57).limit(5).all
      end
    end

    describe "by three columns" do
      it "should limit the dataset appropriately when a starting point is not given" do
        result = DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk).seek_paginate(5).all
        result.should == DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk).limit(5).all

        # Then in reverse:
        results = DB[:seek].order(Sequel.desc(:seek__non_nullable_1), Sequel.desc(:seek__non_nullable_2), Sequel.desc(:seek__pk)).seek_paginate(5).all
        results.should == DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:non_nullable_2), Sequel.desc(:pk)).limit(5).all
      end

      it "should page properly when given a starting point" do
        trio = DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk).offset(56).get([:non_nullable_1, :non_nullable_2, :pk])

        result = DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk).seek_paginate(5, after: trio).all
        result.should == DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk).offset(57).limit(5).all

        # Then in reverse:
        pair = DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:non_nullable_2), Sequel.desc(:pk)).offset(56).get([:non_nullable_1, :non_nullable_2, :pk])

        result = DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:non_nullable_2), Sequel.desc(:pk)).seek_paginate(5, after: pair).all
        result.should == DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:non_nullable_2), Sequel.desc(:pk)).offset(57).limit(5).all
      end
    end
  end

  describe "when ordering in different directions" do
    it "by two columns should page properly when given a starting point" do
      datasets = [
        DB[:seek].order(:non_nullable_1, Sequel.desc(:pk)),
        DB[:seek].order(Sequel.desc(:non_nullable_1), :pk)
      ]

      datasets.each do |ds|
        pair = ds.offset(56).get([:non_nullable_1, :pk])
        result = ds.seek_paginate(5, after: pair).all
        result.should == ds.offset(57).limit(5).all
      end
    end

    it "by three columns should page properly when given a starting point" do
      datasets = [
        DB[:seek].order(Sequel.desc(:non_nullable_1), :non_nullable_2, :pk),
        DB[:seek].order(:non_nullable_1, Sequel.desc(:non_nullable_2), :pk),
        DB[:seek].order(:non_nullable_1, :non_nullable_2, Sequel.desc(:pk)),
        DB[:seek].order(:non_nullable_1, Sequel.desc(:non_nullable_2), Sequel.desc(:pk)),
        DB[:seek].order(Sequel.desc(:non_nullable_1), :non_nullable_2, Sequel.desc(:pk)),
        DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:non_nullable_2), :pk)
      ]

      datasets.each do |ds|
        trio = ds.offset(56).get([:non_nullable_1, :non_nullable_2, :pk])
        result = ds.seek_paginate(5, after: trio).all
        result.should == ds.offset(57).limit(5).all
      end
    end
  end

  describe "when ordering by nullable columns" do
    describe "when ordering by two columns, the first of which is nullable" do
      it "should page properly from a non-null starting point"

      it "should page properly from a null starting point"
    end

    describe "when ordering by three columns, the first two of which are nullable" do
      it "should page properly from a non-null starting point"

      it "should page properly from a null starting point"
    end
  end

  describe "when ordering with nulls first/last settings" do
    describe "by two columns" do
      it "should page properly from a non-null starting point"

      it "should page properly from a null starting point"
    end

    describe "by three columns" do
      it "should page properly from a non-null starting point"

      it "should page properly from a null starting point"
    end
  end

  describe "random testing" do
    it "should handle any permutation of accepted ordering criteria" do
      100.times do |i|
        # Will add nullable columns when those are better supported.
        possible_columns = [:non_nullable_1, :non_nullable_2]
        columns = possible_columns.sample(random(possible_columns.count))

        all_columns = columns + [:pk]

        offset = random(999)

        ordering = all_columns.map do |column|
          rand < 0.5 ? Sequel.asc(column) : Sequel.desc(column)
        end

        after = DB[:seek].order(*ordering).offset(offset).get(all_columns)

        expected = DB[:seek].order(*ordering).offset(offset + 1).limit(10).all
        actual   = DB[:seek].order(*ordering).seek_paginate(10, after: after).all

        actual.should == expected
      end
    end
  end
end
