require 'spec_helper'

class SeekPaginationSpec < Minitest::Spec
  SEEK_COUNT = DB[:seek].count

  def assert_equal_results(ds1, ds2)
    assert_equal ds1.all, ds2.all
  end

  def assert_error_message(message, &block)
    error = assert_raises(Sequel::SeekPagination::Error, &block)
    assert_equal message, error.message
  end

  class << self
    def it_should_seek_paginate_properly(ordering)
      columns = ordering.map do |order|
                  case order
                  when Symbol then order
                  when Sequel::SQL::OrderedExpression then order.expression
                  else raise "Bad order! #{order.inspect}"
                  end
                end

      [:plain, :model].each do |dataset_type|
        describe "for a #{dataset_type} dataset" do
          dataset = dataset_type == :plain ? DB[:seek] : SeekModel
          dataset = dataset.order(*ordering)

          # Can't pass any random expression to #get, so give them all aliases.
          gettable = columns.zip(:a..:z).map{|c,a| c.as(a)}

          it "should limit the dataset appropriately when a starting point is not given" do
            assert_equal_results dataset.limit(10),
                                 dataset.seek_paginate(10)
          end

          it "should page properly when given a point to start from/after" do
            offset = rand(SEEK_COUNT)
            values = dataset.offset(offset).get(gettable)

            assert_equal_results dataset.offset(offset).limit(100),
                                 dataset.seek_paginate(100, from: values)

            assert_equal_results dataset.offset(offset + 1).limit(100),
                                 dataset.seek_paginate(100, after: values)

            if columns.length == 1
              # Should wrap values in an array if necessary
              assert_equal_results dataset.offset(offset).limit(100),
                                   dataset.seek_paginate(100, from: values.first)

              assert_equal_results dataset.offset(offset + 1).limit(100),
                                   dataset.seek_paginate(100, after: values.first)
            end
          end

          it "should return correct results when nullability information is provided" do
            offset = rand(SEEK_COUNT)
            values = dataset.offset(offset).get(gettable)

            assert_equal_results dataset.offset(offset).limit(100),
                                 dataset.seek_paginate(100, from: values, not_null: [:id, :non_nullable_1, :non_nullable_2])

            assert_equal_results dataset.offset(offset + 1).limit(100),
                                 dataset.seek_paginate(100, after: values, not_null: [:id, :non_nullable_1, :non_nullable_2])
          end

          if dataset_type == :model
            it "should page properly when given a primary key to start from/after" do
              offset = rand(SEEK_COUNT)
              id     = dataset.offset(offset).get(:id)

              assert_equal_results dataset.offset(offset).limit(100),
                                   dataset.seek_paginate(100, from_pk: id)

              assert_equal_results dataset.offset(offset + 1).limit(100),
                                   dataset.seek_paginate(100, after_pk: id)
            end
          end
        end
      end
    end
  end

  describe "for ordering by a single not-null column in either order" do
    [:id.asc, :id.desc].each do |o1|
      it_should_seek_paginate_properly [o1]
    end
  end

  describe "for ordering by two not-null columns in any order" do
    [:not_nullable_1.asc, :not_nullable_1.desc].each do |o1|
      [:id.asc, :id.desc].each do |o2|
        it_should_seek_paginate_properly [o1, o2]
      end
    end
  end

  describe "for ordering by three not-null columns in any order" do
    [:not_nullable_1.asc, :not_nullable_1.desc].each do |o1|
      [:not_nullable_2.asc, :not_nullable_2.desc].each do |o2|
        [:id.asc, :id.desc].each do |o3|
          it_should_seek_paginate_properly [o1, o2, o3]
        end
      end
    end
  end

  describe "for ordering by a nullable column" do
    # We still tack on :id because the ordering needs to be unique.
    [:nullable_1.asc, :nullable_1.desc, :nullable_1.asc(nulls: :first), :nullable_1.desc(nulls: :last)].each do |o1|
      [:id.asc, :id.desc].each do |o2|
        it_should_seek_paginate_properly [o1, o2]
      end
    end
  end

  describe "for ordering by multiple nullable columns" do
    # We still tack on :id because the ordering needs to be unique.
    [:nullable_1.asc, :nullable_1.desc, :nullable_1.asc(nulls: :first), :nullable_1.desc(nulls: :last)].each do |o1|
      [:nullable_2.asc, :nullable_2.desc, :nullable_2.asc(nulls: :first), :nullable_2.desc(nulls: :last)].each do |o2|
        [:id.asc, :id.desc].each do |o3|
          it_should_seek_paginate_properly [o1, o2, o3]
        end
      end
    end
  end

  describe "for ordering by a mix of nullable and not-nullable columns" do
    20.times do
      columns = [
        [:not_nullable_1.asc, :not_nullable_1.desc],
        [:not_nullable_2.asc, :not_nullable_2.desc],
        [:nullable_1.asc, :nullable_1.desc, :nullable_1.asc(nulls: :first), :nullable_1.desc(nulls: :last)],
        [:nullable_2.asc, :nullable_2.desc, :nullable_2.asc(nulls: :first), :nullable_2.desc(nulls: :last)],
      ]

      testing_columns = columns.sample(rand(columns.count) + 1).map(&:sample)
      testing_columns << [:id.asc, :id.desc].sample

      it_should_seek_paginate_properly(testing_columns)
    end
  end

  describe "for ordering by a mix of expressions and columns" do
    20.times do
      columns = [
        [:not_nullable_1.asc, :not_nullable_1.desc, (:not_nullable_1.sql_number % 10).asc, (:not_nullable_1.sql_number % 10).desc],
        [:not_nullable_2.asc, :not_nullable_2.desc, (:not_nullable_2.sql_number % 10).asc, (:not_nullable_2.sql_number % 10).desc],
        [:nullable_1.asc, :nullable_1.desc, :nullable_1.asc(nulls: :first), :nullable_1.desc(nulls: :last), (:nullable_1.sql_number % 10).asc, (:nullable_1.sql_number % 10).desc, (:nullable_1.sql_number % 10).asc(nulls: :first), (:nullable_1.sql_number % 10).desc(nulls: :last)],
        [:nullable_2.asc, :nullable_2.desc, :nullable_2.asc(nulls: :first), :nullable_2.desc(nulls: :last), (:nullable_2.sql_number % 10).asc, (:nullable_2.sql_number % 10).desc, (:nullable_2.sql_number % 10).asc(nulls: :first), (:nullable_2.sql_number % 10).desc(nulls: :last)],
      ]

      testing_columns = columns.sample(rand(columns.count) + 1).map(&:sample)
      testing_columns << [:id.asc, :id.desc].sample

      it_should_seek_paginate_properly(testing_columns)
    end
  end

  it "should work for order clauses of many types" do
    datasets = [
      DB[:seek].order(:id),
      DB[:seek].order(:seek__id),
      DB[:seek].order(:id.asc),
      DB[:seek].order(:seek__id.asc),
      DB[:seek].order(:id.desc).reverse_order,
      DB[:seek].order(:seek__id.desc).reverse_order,
    ]

    # With point to start from/after:
    id = DB[:seek].order(:id).offset(56).get(:id)

    datasets.each do |dataset|
      assert_equal_results DB[:seek].order(:id).limit(5),
                           dataset.seek_paginate(5)

      assert_equal_results DB[:seek].order(:id).offset(56).limit(5),
                           dataset.seek_paginate(5, from: id)

      assert_equal_results DB[:seek].order(:id).offset(57).limit(5),
                           dataset.seek_paginate(5, after: id)
    end
  end

  it "should raise an error if the dataset is not ordered" do
    assert_error_message("cannot seek_paginate on a dataset with no order") { DB[:seek].seek_paginate(30) }
  end

  it "should raise an error if the dataset is not ordered" do
    assert_error_message("cannot pass both :from and :after params to seek_paginate") { DB[:seek].order(:id).seek_paginate(30, from: 3, after: 4) }
  end

  it "should raise an error if given the wrong number of values to from or after" do
    assert_error_message("passed the wrong number of values in the :from option to seek_paginate")  { DB[:seek].order(:id, :nullable_1).seek_paginate(30, from:  [3]) }
    assert_error_message("passed the wrong number of values in the :after option to seek_paginate") { DB[:seek].order(:id, :nullable_1).seek_paginate(30, after: [3]) }
    assert_error_message("passed the wrong number of values in the :from option to seek_paginate")  { DB[:seek].order(:id, :nullable_1).seek_paginate(30, from:  [3, 4, 5]) }
    assert_error_message("passed the wrong number of values in the :after option to seek_paginate") { DB[:seek].order(:id, :nullable_1).seek_paginate(30, after: [3, 4, 5]) }
  end

  it "should raise an error if from_pk or after_pk are passed to a dataset without an associated model" do
    assert_error_message("passed the :from_pk option to seek_paginate on a dataset that doesn't have an associated model") { DB[:seek].order(:id, :nullable_1).seek_paginate(30, from_pk: 3) }
    assert_error_message("passed the :after_pk option to seek_paginate on a dataset that doesn't have an associated model") { DB[:seek].order(:id, :nullable_1).seek_paginate(30, after_pk: 3) }
  end

  describe "when chained from a model" do
    it "should be able to determine from the schema what columns are not null" do
      assert_equal %(SELECT * FROM "seek" WHERE (("not_nullable_1", "not_nullable_2", "id") > (1, 2, 3)) ORDER BY "not_nullable_1", "not_nullable_2", "id" LIMIT 5),
        SeekModel.order(:not_nullable_1, :not_nullable_2, :id).seek_paginate(5, after: [1, 2, 3]).sql
    end
  end
end
