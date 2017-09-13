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
          dataset = dataset.order(*ordering).limit(100)

          # Can't pass any random expression to #get, so give them all aliases.
          gettable = columns.zip(:a..:z).map{|c,a| Sequel.as(c, a)}

          it "should page properly when given a point to start from/after" do
            offset = rand(SEEK_COUNT)
            values = dataset.offset(offset).get(gettable)

            assert_equal_results dataset.offset(offset),
                                 dataset.seek(value: values, include_exact_match: true)

            assert_equal_results dataset.offset(offset + 1),
                                 dataset.seek(value: values)

            if columns.length == 1
              # Should wrap values in an array if necessary
              assert_equal_results dataset.offset(offset),
                                   dataset.seek(value: values.first, include_exact_match: true)

              assert_equal_results dataset.offset(offset + 1),
                                   dataset.seek(value: values.first)
            end
          end

          it "should return correct results when nullability information is provided" do
            offset = rand(SEEK_COUNT)
            values = dataset.offset(offset).get(gettable)

            assert_equal_results dataset.offset(offset),
                                 dataset.seek(value: values, include_exact_match: true, not_null: [:id, :non_nullable_1, :non_nullable_2])

            assert_equal_results dataset.offset(offset + 1),
                                 dataset.seek(value: values, not_null: [:id, :non_nullable_1, :non_nullable_2])
          end

          if dataset_type == :model
            it "should page properly when given a primary key to start from/after" do
              offset = rand(SEEK_COUNT)
              id     = dataset.offset(offset).get(:id)

              assert_equal_results dataset.offset(offset),
                                   dataset.seek(pk: id, include_exact_match: true)

              assert_equal_results dataset.offset(offset + 1),
                                   dataset.seek(pk: id)
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
      DB[:seek].order(Sequel.qualify(:seek, :id)),
      DB[:seek].order(Sequel.asc(:id)),
      DB[:seek].order(Sequel.asc(Sequel.qualify(:seek, :id))),
      DB[:seek].order(Sequel.desc(:id)).reverse_order,
      DB[:seek].order(Sequel.desc(Sequel.qualify(:seek, :id))).reverse_order,
    ]

    # With point to start from/after:
    id = DB[:seek].order(:id).offset(56).get(:id)

    datasets.each do |dataset|
      assert_equal_results DB[:seek].order(:id).offset(56).limit(5),
                           dataset.limit(5).seek(value: id, include_exact_match: true)

      assert_equal_results DB[:seek].order(:id).offset(57).limit(5),
                           dataset.limit(5).seek(value: id)
    end
  end

  it "should raise an error if the dataset is not ordered" do
    assert_error_message("cannot call #seek on a dataset with no order") { DB[:seek].seek(value: 3) }
  end

  it "should raise an error unless exactly one of :value and :pk is passed" do
    assert_error_message("must pass exactly one of :value and :pk to #seek") { DB[:seek].seek }
    assert_error_message("must pass exactly one of :value and :pk to #seek") { DB[:seek].seek(value: 3, pk: 3) }
  end

  it "should raise an error if given the wrong number of values" do
    assert_error_message("passed the wrong number of values to #seek") { DB[:seek].order(:id, :nullable_1).seek(value: 3) }
    assert_error_message("passed the wrong number of values to #seek") { DB[:seek].order(:id, :nullable_1).seek(value: [3]) }
    assert_error_message("passed the wrong number of values to #seek") { DB[:seek].order(:id, :nullable_1).seek(value: [3, 4, 5]) }
  end

  it "should raise an error if from_pk or after_pk are passed to a dataset without an associated model" do
    assert_error_message("attempted a primary key lookup on a dataset that doesn't have an associated model") { DB[:seek].order(:id, :nullable_1).seek(pk: 3) }
  end

  describe "when chained from a model" do
    it "should be able to determine from the schema what columns are not null" do
      assert_equal %(SELECT * FROM "seek" WHERE (("not_nullable_1", "not_nullable_2", "id") > (1, 2, 3)) ORDER BY "not_nullable_1", "not_nullable_2", "id" LIMIT 5),
        SeekModel.order(:not_nullable_1, :not_nullable_2, :id).seek(value: [1, 2, 3]).limit(5).sql
    end

    it "shouldn't be fooled by table-qualified orderings" do
      assert_equal %(SELECT * FROM "seek" WHERE (("seek"."not_nullable_1", "seek"."not_nullable_2", "seek"."id") > (1, 2, 3)) ORDER BY "seek"."not_nullable_1", "seek"."not_nullable_2", "seek"."id" LIMIT 5),
        SeekModel.order(Sequel.qualify(:seek, :not_nullable_1), Sequel.qualify(:seek, :not_nullable_2), Sequel.qualify(:seek, :id)).seek(value: [1, 2, 3]).limit(5).sql
    end

    describe "when passed a pk and no record is found" do
      it "should default to raising an error" do
        assert_raises(Sequel::NoMatchingRow) do
          SeekModel.order(:id).seek(pk: -45)
        end
      end

      it "should support returning nil" do
        assert_nil SeekModel.order(:id).seek(pk: -45, missing_pk: :return_nil)
      end

      it "should support ignoring the condition" do
        ds = SeekModel.order(:id).seek(pk: -45, missing_pk: :ignore)

        assert_equal SeekModel.order(:id), ds
      end

      it "should support nullifying the dataset" do
        ds = SeekModel.order(:id).seek(pk: -45, missing_pk: :nullify)

        assert_equal [], ds.all
        assert_equal 0, ds.count
      end

      it "should raise when an unsupported option is passed" do
        assert_error_message "passed an invalid argument for missing_pk: :nonexistent_option" do
          SeekModel.order(:id).seek(pk: -45, missing_pk: :nonexistent_option)
        end
      end
    end
  end
end
