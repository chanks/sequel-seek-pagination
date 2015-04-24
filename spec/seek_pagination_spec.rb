require 'spec_helper'

describe Sequel::SeekPagination do
  before do
    DB.drop_table? :seek

    DB.create_table :seek do
      integer :a, null: false
      integer :b
      integer :c, null: false

      primary_key [:c]
    end

    @results = [
      [1,   3,  1],
      [1,   3,  2],
      [1,   4,  3],
      [1,   5,  4],
      [1,   5,  5],
      [1,   5,  6],
      [1,   6,  7],
      [1,   6,  8],
      [1, nil,  9],
      [1, nil, 10],
      [1, nil, 11],
      [1, nil, 12],
      [2,   1, 13],
      [2,   1, 14],
      [2,   2, 15],
      [2,   2, 16],
      [2,   4, 17],
      [2,   4, 18],
      [2,   4, 19],
      [2,   5, 20],
      [2,   5, 21],
      [2,   6, 22],
      [2, nil, 23],
      [2, nil, 24],
    ].map{|a, b, c| {a: a, b: b, c: c}}

    DB[:seek].multi_insert(@results)
  end

  it "should raise an error if the dataset is not ordered" do
    proc {
      DB[:seek].seek_paginate(30)
    }.should raise_error Sequel::SeekPagination::Error, /cannot seek paginate on a dataset with no order/
  end

  it "should raise an error on a dataset with mixed ordering" do
    proc {
      DB[:seek].order(:c, Sequel.desc(:a)).seek_paginate(30)
    }.should raise_error Sequel::SeekPagination::Error, /cannot seek paginate on a query ordering by multiple columns in different directions/
  end

  describe "when ordering by a single, unique, non-null column" do
    it "should limit the dataset appropriately when a starting point is not given" do
      datasets = [
        DB[:seek].order(:c),
        DB[:seek].order(:seek__c),
        DB[:seek].order(Sequel.asc(:c)),
        DB[:seek].order(Sequel.asc(:seek__c)),
        DB[:seek].order(Sequel.desc(:c)).reverse_order
      ]

      datasets.each do |dataset|
        dataset.seek_paginate(5).all.should == @results[0..4]
      end

      # Then in reverse:
      DB[:seek].order(Sequel.desc(:c)).seek_paginate(5).all.should == @results[-5..-1].reverse
    end

    it "should page properly when given a starting point" do
      datasets = [
        DB[:seek].order(:c),
        DB[:seek].order(:seek__c),
        DB[:seek].order(Sequel.asc(:c)),
        DB[:seek].order(Sequel.asc(:seek__c)),
        DB[:seek].order(Sequel.desc(:c)).reverse_order
      ]

      datasets.each do |dataset|
        result = dataset.seek_paginate(5, after: @results[2][:c]).all
        result.should == @results[3..7]
      end

      # Then in reverse:
      result = DB[:seek].order(Sequel.desc(:c)).seek_paginate(5, after: @results[9][:c]).all
      result.should == @results[4..8].reverse
    end
  end

  describe "when ordering by multiple, unique columns, all ordered in the same direction" do
    it "should limit the dataset appropriately when a starting point is not given" do
      datasets = [
        DB[:seek].order(:a, :c),
        DB[:seek].order(:seek__a, :seek__c),
        DB[:seek].order(Sequel.asc(:a), Sequel.asc(:c)),
        DB[:seek].order(Sequel.asc(:seek__a), Sequel.asc(:seek__c)),
        DB[:seek].order(Sequel.desc(:seek__a), Sequel.desc(:seek__c)).reverse_order
      ]

      datasets.each do |dataset|
        dataset.seek_paginate(5).all.should == @results[0..4]
      end

      # Then in reverse:
      results = DB[:seek].order(Sequel.desc(:seek__a), Sequel.desc(:seek__c)).seek_paginate(5).all
      results.should == @results[-5..-1].reverse
    end

    it "should page properly when given a starting point" do
      datasets = [
        DB[:seek].order(:a, :c),
        DB[:seek].order(:seek__a, :seek__c),
        DB[:seek].order(Sequel.asc(:a), Sequel.asc(:c)),
        DB[:seek].order(Sequel.asc(:seek__a), Sequel.asc(:seek__c)),
        DB[:seek].order(Sequel.desc(:seek__a), Sequel.desc(:seek__c)).reverse_order
      ]

      datasets.each do |dataset|
        results = dataset.seek_paginate(5, after: @results[3].values_at(:a, :c)).all
        results.should == @results[4..8]
      end

      # Then in reverse:
      results = DB[:seek].order(Sequel.desc(:seek__a), Sequel.desc(:seek__c)).seek_paginate(5, after: @results[19].values_at(:a, :c)).all
      results.should == @results[14..18].reverse
    end
  end
end
