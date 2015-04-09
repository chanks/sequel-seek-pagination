require 'spec_helper'

describe Sequel::SeekPagination do
  it "should limit the dataset appropriately when a starting point is not given" do
    datasets = [
      DB[:seek].order(:id),
      DB[:seek].order(Sequel.asc(:id)),
      DB[:seek].order(Sequel.desc(:id)).reverse_order
    ]

    datasets.each do |dataset|
      result = dataset.seek_paginate(30).all
      result.should == (1..30).map{|i| {id: i}}
    end

    # Then in reverse:
    result = DB[:seek].order(Sequel.desc(:id)).seek_paginate(30).all
    result.should == (9971..10000).map{|i| {id: i}}.reverse
  end
end
