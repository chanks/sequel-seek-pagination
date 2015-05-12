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
          length = sets.length

          dataset.where(
            Sequel.&(
              *length.times.map { |i|
                is_last = i == length - 1
                conditions = sets[0..i]

                if i.zero?
                  col, value = conditions[0]
                  ineq(col, value, eq: !is_last)
                else
                  c0, v0 = conditions[-2]
                  c1, v1 = conditions[-1]

                  list = [Sequel.&({c0.name => v0}, ineq(c1, v1, eq: !is_last))]
                  conditions[0..-2].each do |c, v|
                    list << ineq(c, v, eq: false)
                  end
                  Sequel.|(*list)
                end
              }
            )
          )
        end

        private

        def ineq(column, value, eq: true)
          method = "#{column.direction == :asc ? '>' : '<'}#{'=' if eq}"
          Sequel.virtual_row { |o| o.__send__(method, column.name, value) }
        end
      end
    end
  end

  Dataset.register_extension(:seek_pagination, SeekPagination)
end
