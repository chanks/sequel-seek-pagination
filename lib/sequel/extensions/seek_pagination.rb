require 'sequel'
require 'sequel/extensions/seek_pagination/version'

module Sequel
  module SeekPagination
    class Error < StandardError; end

    def seek_paginate(count, after: nil)
      order = opts[:order]

      if order.nil? || order.length.zero?
        raise Error, "cannot seek paginate on a dataset with no order"
      end

      orders = order.map { |o| OrderedColumn.new(o) }

      ds = limit(count)

      if after
        OrderedColumn.apply(ds, orders.zip([*after]))
      else
        ds
      end
    end

    private

    class OrderedColumn
      attr_reader :name, :direction

      def initialize(order)
        @name, @direction = case order
                            when Symbol                         then [order, :asc]
                            when Sequel::SQL::OrderedExpression then [order.expression, order.descending ? :desc : :asc]
                            else raise "Unrecognized order!: #{order.inspect}"
                            end
      end

      class << self
        def apply(dataset, sets)
          first_col, first_value = sets[0]
          last_col = sets[-1][0]

          ds = dataset.where(ineq(first_col.direction, first_col.name, first_value, eq: first_col != last_col))

          sets.each_cons(2) do |(col_a, col_a_value), (col_b, col_b_value)|
            ds = ds.where do |o|
              Sequel.|(
                ineq(col_a.direction, col_a.name, col_a_value, eq: false),
                Sequel.&(
                  {col_a.name => col_a_value},
                  ineq(col_b.direction, col_b.name, col_b_value, eq: col_b != last_col)
                )
              )
            end
          end

          ds
        end

        private

        def ineq(direction, name, value, eq: true)
          method = "#{direction == :asc ? '>' : '<'}#{'=' if eq}"
          Sequel.virtual_row { |o| o.__send__(method, name, value) }
        end
      end
    end
  end

  Dataset.register_extension(:seek_pagination, SeekPagination)
end
