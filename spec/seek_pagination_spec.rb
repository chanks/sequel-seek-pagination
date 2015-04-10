require 'spec_helper'

describe Sequel::SeekPagination do
  it "should raise an error if the dataset is not ordered" do
    proc {
      DB[:seek].seek_paginate(30)
    }.should raise_error Sequel::SeekPagination::Error, /cannot seek paginate on a dataset with no order/
  end

  it "should raise an error on a dataset with mixed ordering" do
    proc {
      DB[:seek].order(:id, Sequel.desc(:col1)).seek_paginate(30)
    }.should raise_error Sequel::SeekPagination::Error, /cannot seek paginate on a query ordering by multiple columns in different directions/
  end

  describe "when ordering by a single, unique, non-null column" do
    it "should limit the dataset appropriately when a starting point is not given" do
      datasets = [
        DB[:seek].order(:id),
        DB[:seek].order(:seek__id),
        DB[:seek].order(Sequel.asc(:id)),
        DB[:seek].order(Sequel.asc(:seek__id)),
        DB[:seek].order(Sequel.desc(:id)).reverse_order
      ]

      datasets.each do |dataset|
        result = dataset.seek_paginate(30).all
        result.map{|r| r[:id]}.should == (1..30).to_a
      end

      # Then in reverse:
      result = DB[:seek].order(Sequel.desc(:id)).seek_paginate(30).all
      result.map{|r| r[:id]}.should == (971..1000).to_a.reverse
    end

    it "should page properly when given a starting point" do
      datasets = [
        DB[:seek].order(:id),
        DB[:seek].order(:seek__id),
        DB[:seek].order(Sequel.asc(:id)),
        DB[:seek].order(Sequel.asc(:seek__id)),
        DB[:seek].order(Sequel.desc(:id)).reverse_order
      ]

      datasets.each do |dataset|
        result = dataset.seek_paginate(30, after: 79).all
        result.map{|r| r[:id]}.should == (80..109).to_a
      end

      # Then in reverse:
      result = DB[:seek].order(Sequel.desc(:id)).seek_paginate(30, after: 789).all
      result.map{|r| r[:id]}.should == (759..788).to_a.reverse
    end
  end

  describe "when ordering by multiple, unique columns, all ordered in the same direction" do
    it "should limit the dataset appropriately when a starting point is not given" do
      datasets = [
        DB[:seek].order(:col1, :id),
        DB[:seek].order(:seek__col1, :seek__id),
        DB[:seek].order(Sequel.asc(:col1), Sequel.asc(:id)),
        DB[:seek].order(Sequel.asc(:seek__col1), Sequel.asc(:seek__id)),
        DB[:seek].order(Sequel.desc(:seek__col1), Sequel.desc(:seek__id)).reverse_order
      ]

      datasets.each do |dataset|
        results = dataset.seek_paginate(30).all.map{|r| [r[:col1], r[:id]]}
        results.should == results.sort
      end

      # Then in reverse:
      results = DB[:seek].order(Sequel.desc(:seek__col1), Sequel.desc(:seek__id)).seek_paginate(30).all.map{|r| [r[:col1], r[:id]]}
      results.should == results.sort.reverse
    end

    it "should page properly when given a starting point" do
      datasets = [
        DB[:seek].order(:col1, :id),
        DB[:seek].order(:seek__col1, :seek__id),
        DB[:seek].order(Sequel.asc(:col1), Sequel.asc(:id)),
        DB[:seek].order(Sequel.asc(:seek__col1), Sequel.asc(:seek__id)),
        DB[:seek].order(Sequel.desc(:seek__col1), Sequel.desc(:seek__id)).reverse_order
      ]

      record = DB[:seek].order{random{}}.first

      datasets.each do |dataset|
        results = dataset.seek_paginate(30, after: [record[:col1], record[:id]]).all.map{|r| [r[:col1], r[:id]]}
        results.should == results.sort
      end

      # Then in reverse:
      results = DB[:seek].order(Sequel.desc(:seek__col1), Sequel.desc(:seek__id)).seek_paginate(30, after: [record[:col1], record[:id]]).all.map{|r| [r[:col1], r[:id]]}
      results.should == results.sort.reverse
    end
  end
end
