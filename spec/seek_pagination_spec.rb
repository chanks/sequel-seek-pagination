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
    def it_should_seek_properly(ordering)
      columns = ordering.map do |order|
                  case order
                  when Sequel::SQL::OrderedExpression then order.expression
                  else order
                  end
                end

      [:plain, :model].each do |dataset_type|
        describe "for a #{dataset_type} dataset" do
          dataset = dataset_type == :plain ? DB[:seek] : SeekModel
          dataset = dataset.order(*ordering)

          # Can't pass any random expression to #get, so give them all aliases.
          gettable = columns.zip(:a..:z).map{|c,a| Sequel.as(c, a)}

          it "should page properly when given a point to start from/after" do
            offset = rand(SEEK_COUNT)
            values = dataset.offset(offset).get(gettable)

            assert_equal_results dataset.offset(offset).limit(100),
                                 dataset.limit(100).seek(values, with_base: true)

            assert_equal_results dataset.offset(offset + 1).limit(100),
                                 dataset.limit(100).seek(values)

            if columns.length == 1
              # Should wrap values in an array if necessary
              assert_equal_results dataset.offset(offset).limit(100),
                                   dataset.limit(100).seek(values.first, with_base: true)

              assert_equal_results dataset.offset(offset + 1).limit(100),
                                   dataset.limit(100).seek(values.first)
            end
          end

          it "should return correct results when nullability information is provided" do
            offset = rand(SEEK_COUNT)
            values = dataset.offset(offset).get(gettable)

            assert_equal_results dataset.offset(offset).limit(100),
                                 dataset.limit(100).seek(values, not_null: [:id, :non_nullable_1, :non_nullable_2], with_base: true)

            assert_equal_results dataset.offset(offset + 1).limit(100),
                                 dataset.limit(100).seek(values, not_null: [:id, :non_nullable_1, :non_nullable_2])
          end

          if dataset_type == :model
            it "should page properly when given a primary key to start from/after" do
              offset = rand(SEEK_COUNT)
              id     = dataset.offset(offset).get(:id)

              assert_equal_results dataset.offset(offset).limit(100),
                                   dataset.limit(100).seek(id, with_base: true, by_pk: true)

              assert_equal_results dataset.offset(offset + 1).limit(100),
                                   dataset.limit(100).seek(id, by_pk: true)
            end
          end

          it "should work backwards" do
            limit  = 50
            offset = rand(SEEK_COUNT - limit) + limit

            values = dataset.offset(offset).get(gettable)
            assert_equal dataset.offset(offset - limit).limit(limit).all,
              dataset.limit(limit).seek(values, back: true).all.reverse

            if dataset_type == :model
              id     = dataset.offset(offset).get(:id)
              assert_equal dataset.offset(offset - limit).limit(limit).all,
                dataset.limit(limit).seek(id, by_pk: true, back: true).all.reverse
            end
          end
        end
      end
    end
  end

  describe "for ordering by a single not-null column in either order" do
    [Sequel.asc(:id), Sequel.desc(:id)].each do |o1|
      it_should_seek_properly [o1]
    end
  end

  describe "for ordering by two not-null columns in any order" do
    [Sequel.asc(:not_nullable_1), Sequel.desc(:not_nullable_1)].each do |o1|
      [Sequel.asc(:id), Sequel.desc(:id)].each do |o2|
        it_should_seek_properly [o1, o2]
      end
    end
  end

  describe "for ordering by three not-null columns in any order" do
    [Sequel.asc(:not_nullable_1), Sequel.desc(:not_nullable_1)].each do |o1|
      [Sequel.asc(:not_nullable_2), Sequel.desc(:not_nullable_2)].each do |o2|
        [Sequel.asc(:id), Sequel.desc(:id)].each do |o3|
          it_should_seek_properly [o1, o2, o3]
        end
      end
    end
  end

  describe "for ordering by a nullable column" do
    # We still tack on :id because the ordering needs to be unique.
    [Sequel.asc(:nullable_1), Sequel.desc(:nullable_1), Sequel.asc(:nullable_1, nulls: :first), Sequel.desc(:nullable_1, nulls: :last)].each do |o1|
      [Sequel.asc(:id), Sequel.desc(:id)].each do |o2|
        it_should_seek_properly [o1, o2]
      end
    end
  end

  describe "for ordering by multiple nullable columns" do
    # We still tack on :id because the ordering needs to be unique.
    [Sequel.asc(:nullable_1), Sequel.desc(:nullable_1), Sequel.asc(:nullable_1, nulls: :first), Sequel.desc(:nullable_1, nulls: :last)].each do |o1|
      [Sequel.asc(:nullable_2), Sequel.desc(:nullable_2), Sequel.asc(:nullable_2, nulls: :first), Sequel.desc(:nullable_2, nulls: :last)].each do |o2|
        [Sequel.asc(:id), Sequel.desc(:id)].each do |o3|
          it_should_seek_properly [o1, o2, o3]
        end
      end
    end
  end

  describe "for ordering by a mix of nullable and not-nullable columns" do
    20.times do
      columns = [
        [:not_nullable_1, Sequel.asc(:not_nullable_1), Sequel.desc(:not_nullable_1)],
        [:not_nullable_2, Sequel.asc(:not_nullable_2), Sequel.desc(:not_nullable_2)],
        [:nullable_1, Sequel.asc(:nullable_1), Sequel.desc(:nullable_1), Sequel.asc(:nullable_1, nulls: :first), Sequel.desc(:nullable_1, nulls: :last)],
        [:nullable_2, Sequel.asc(:nullable_2), Sequel.desc(:nullable_2), Sequel.asc(:nullable_2, nulls: :first), Sequel.desc(:nullable_2, nulls: :last)],
      ]

      testing_columns = columns.sample(rand(columns.count) + 1).map(&:sample)
      testing_columns << [:id, Sequel.asc(:id), Sequel.desc(:id)].sample

      it_should_seek_properly(testing_columns)
    end
  end

  describe "for ordering by a mix of expressions and columns" do
    20.times do
      columns = [
        [:not_nullable_1, Sequel.asc(:not_nullable_1), Sequel.desc(:not_nullable_1), Sequel.expr(:not_nullable_1).sql_number % 10, Sequel.asc(Sequel.expr(:not_nullable_1).sql_number % 10), Sequel.desc(Sequel.expr(:not_nullable_1).sql_number % 10)],
        [:not_nullable_2, Sequel.asc(:not_nullable_2), Sequel.desc(:not_nullable_2), Sequel.expr(:not_nullable_2).sql_number % 10, Sequel.asc(Sequel.expr(:not_nullable_2).sql_number % 10), Sequel.desc(Sequel.expr(:not_nullable_2).sql_number % 10)],
        [:nullable_1, Sequel.asc(:nullable_1), Sequel.desc(:nullable_1), Sequel.asc(:nullable_1, nulls: :first), Sequel.desc(:nullable_1, nulls: :last), Sequel.expr(:nullable_1).sql_number % 10, Sequel.asc(Sequel.expr(:nullable_1).sql_number % 10), (Sequel.expr(:nullable_1).sql_number % 10).desc, Sequel.asc(Sequel.expr(:nullable_1).sql_number % 10, nulls: :first), Sequel.desc(Sequel.expr(:nullable_1).sql_number % 10, nulls: :last)],
        [:nullable_2, Sequel.asc(:nullable_2), Sequel.desc(:nullable_2), Sequel.asc(:nullable_2, nulls: :first), Sequel.desc(:nullable_2, nulls: :last), Sequel.expr(:nullable_2).sql_number % 10, Sequel.asc(Sequel.expr(:nullable_2).sql_number % 10), (Sequel.expr(:nullable_2).sql_number % 10).desc, Sequel.asc(Sequel.expr(:nullable_2).sql_number % 10, nulls: :first), Sequel.desc(Sequel.expr(:nullable_2).sql_number % 10, nulls: :last)],
      ]

      testing_columns = columns.sample(rand(columns.count) + 1).map(&:sample)
      testing_columns << [:id, Sequel.asc(:id), Sequel.desc(:id)].sample

      it_should_seek_properly(testing_columns)
    end
  end

  it "should work for order clauses of many types" do
    datasets = [
      DB[:seek].order(:id),
      DB[:seek].order(:seek__id),
      DB[:seek].order(Sequel.asc(:id)),
      DB[:seek].order(Sequel.asc(:seek__id)),
      DB[:seek].order(Sequel.desc(:id)).reverse_order,
      DB[:seek].order(Sequel.desc(:seek__id)).reverse_order,
    ]

    # With point to start from/after:
    id = DB[:seek].order(:id).offset(56).get(:id)

    datasets.each do |dataset|
      assert_equal_results DB[:seek].order(:id).offset(56).limit(5),
                           dataset.limit(5).seek(id, with_base: true)

      assert_equal_results DB[:seek].order(:id).offset(57).limit(5),
                           dataset.limit(5).seek(id)
    end
  end

  it "should raise an error if the dataset is not ordered" do
    assert_error_message("cannot seek on a dataset with no order") { DB[:seek].limit(30).seek(1) }
  end

  it "should raise an error if given the wrong number of values to seek" do
    assert_error_message("passed the wrong number of values to seek")  { DB[:seek].order(:id, :nullable_1).limit(30).seek([3], with_base: true) }
    assert_error_message("passed the wrong number of values to seek") { DB[:seek].order(:id, :nullable_1).limit(30).seek([3]) }
    assert_error_message("passed the wrong number of values to seek")  { DB[:seek].order(:id, :nullable_1).limit(30).seek([3, 4, 5], with_base: true) }
    assert_error_message("passed the wrong number of values to seek") { DB[:seek].order(:id, :nullable_1).limit(30).seek([3, 4, 5]) }
  end

  it "should raise an error if by_pk are passed to a dataset without an associated model" do
    assert_error_message("passed the :by_pk option to seek on a dataset that doesn't have an associated model") { DB[:seek].order(:id, :nullable_1).limit(30).seek(3, with_base: true, by_pk: true) }
    assert_error_message("passed the :by_pk option to seek on a dataset that doesn't have an associated model") { DB[:seek].order(:id, :nullable_1).limit(30).seek(3, by_pk: true) }
  end

  describe "when chained from a model" do
    it "should be able to determine from the schema what columns are not null" do
      assert_equal %(SELECT * FROM "seek" WHERE (("not_nullable_1", "not_nullable_2", "id") > (1, 2, 3)) ORDER BY "not_nullable_1", "not_nullable_2", "id" LIMIT 5),
        SeekModel.order(:not_nullable_1, :not_nullable_2, :id).limit(5).seek([1, 2, 3]).sql
    end

    it "should raise an error when passed a pk for a record that doesn't exist in the dataset" do
      assert_raises(Sequel::NoMatchingRow) { SeekModel.order(:id).limit(5).seek(-45, by_pk: true) }
      assert_raises(Sequel::NoMatchingRow) { SeekModel.order(:id).limit(5).seek(-45, with_base: true, by_pk: true) }
    end
  end
end
