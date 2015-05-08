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

  it "should raise an error on a dataset with mixed ordering" do
    proc {
      DB[:seek].order(:non_nullable_1, Sequel.desc(:non_nullable_2)).seek_paginate(30)
    }.should raise_error Sequel::SeekPagination::Error, /cannot seek paginate on a query ordering by multiple columns in different directions/
  end

  describe "when ordering by a single, unique, non-null column" do
    it "should limit the dataset appropriately when a starting point is not given" do
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
    end

    it "should page properly when given a starting point" do
      datasets = [
        DB[:seek].order(:pk),
        DB[:seek].order(:seek__pk),
        DB[:seek].order(Sequel.asc(:pk)),
        DB[:seek].order(Sequel.asc(:seek__pk)),
        DB[:seek].order(Sequel.desc(:pk)).reverse_order
      ]

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

  describe "when ordering by two unique columns, ordered in the same direction" do
    it "should limit the dataset appropriately when a starting point is not given" do
      datasets = [
        DB[:seek].order(:non_nullable_1, :pk),
        DB[:seek].order(:seek__non_nullable_1, :seek__pk),
        DB[:seek].order(Sequel.asc(:non_nullable_1), Sequel.asc(:pk)),
        DB[:seek].order(Sequel.asc(:seek__non_nullable_1), Sequel.asc(:seek__pk)),
        DB[:seek].order(Sequel.desc(:seek__non_nullable_1), Sequel.desc(:seek__pk)).reverse_order
      ]

      datasets.each do |dataset|
        result = dataset.seek_paginate(5).all
        result.should == DB[:seek].order(:non_nullable_1, :pk).limit(5).all
      end

      # Then in reverse:
      results = DB[:seek].order(Sequel.desc(:seek__non_nullable_1), Sequel.desc(:seek__pk)).seek_paginate(5).all
      results.should == DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:pk)).limit(5).all
    end

    it "should page properly when given a starting point" do
      datasets = [
        DB[:seek].order(:non_nullable_1, :pk),
        DB[:seek].order(:seek__non_nullable_1, :seek__pk),
        DB[:seek].order(Sequel.asc(:non_nullable_1), Sequel.asc(:pk)),
        DB[:seek].order(Sequel.asc(:seek__non_nullable_1), Sequel.asc(:seek__pk)),
        DB[:seek].order(Sequel.desc(:seek__non_nullable_1), Sequel.desc(:seek__pk)).reverse_order
      ]

      datasets.each do |dataset|
        pair = DB[:seek].order(:non_nullable_1, :pk).offset(56).get([:non_nullable_1, :pk])

        result = dataset.seek_paginate(5, after: pair).all
        result.should == DB[:seek].order(:non_nullable_1, :pk).offset(57).limit(5).all
      end

      # Then in reverse:
      pair = DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:pk)).offset(56).get([:non_nullable_1, :pk])

      result = DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:pk)).seek_paginate(5, after: pair).all
      result.should == DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:pk)).offset(57).limit(5).all
    end
  end

  describe "when ordering by three unique columns, ordered in the same direction" do
    it "should limit the dataset appropriately when a starting point is not given" do
      datasets = [
        DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk),
        DB[:seek].order(:seek__non_nullable_1, :seek__non_nullable_2, :seek__pk),
        DB[:seek].order(Sequel.asc(:non_nullable_1), Sequel.asc(:non_nullable_2), Sequel.asc(:pk)),
        DB[:seek].order(Sequel.asc(:seek__non_nullable_1), Sequel.asc(:seek__non_nullable_2), Sequel.asc(:seek__pk)),
        DB[:seek].order(Sequel.desc(:seek__non_nullable_1), Sequel.desc(:seek__non_nullable_2), Sequel.desc(:seek__pk)).reverse_order
      ]

      datasets.each do |dataset|
        result = dataset.seek_paginate(5).all
        result.should == DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk).limit(5).all
      end

      # Then in reverse:
      results = DB[:seek].order(Sequel.desc(:seek__non_nullable_1), Sequel.desc(:seek__non_nullable_2), Sequel.desc(:seek__pk)).seek_paginate(5).all
      results.should == DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:non_nullable_2), Sequel.desc(:pk)).limit(5).all
    end

    it "should page properly when given a starting point" do
      datasets = [
        DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk),
        DB[:seek].order(:seek__non_nullable_1, :seek__non_nullable_2, :seek__pk),
        DB[:seek].order(Sequel.asc(:non_nullable_1), Sequel.asc(:non_nullable_2), Sequel.asc(:pk)),
        DB[:seek].order(Sequel.asc(:seek__non_nullable_1), Sequel.asc(:seek__non_nullable_2), Sequel.asc(:seek__pk)),
        DB[:seek].order(Sequel.desc(:seek__non_nullable_1), Sequel.desc(:non_nullable_2), Sequel.desc(:seek__pk)).reverse_order
      ]

      datasets.each do |dataset|
        trio = DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk).offset(56).get([:non_nullable_1, :non_nullable_2, :pk])

        result = dataset.seek_paginate(5, after: trio).all
        result.should == DB[:seek].order(:non_nullable_1, :non_nullable_2, :pk).offset(57).limit(5).all
      end

      # Then in reverse:
      pair = DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:non_nullable_2), Sequel.desc(:pk)).offset(56).get([:non_nullable_1, :non_nullable_2, :pk])

      result = DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:non_nullable_2), Sequel.desc(:pk)).seek_paginate(5, after: pair).all
      result.should == DB[:seek].order(Sequel.desc(:non_nullable_1), Sequel.desc(:non_nullable_2), Sequel.desc(:pk)).offset(57).limit(5).all
    end
  end
end
