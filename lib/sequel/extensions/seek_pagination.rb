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
      attr_reader :name, :direction, :nulls

      def initialize(order)
        @name, @direction, @nulls =
          case order
          when Symbol                         then [order, :asc, :last]
          when Sequel::SQL::OrderedExpression then [order.expression, order.descending ? :desc : :asc, order.nulls]
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
                  ineq(col, value, eq: !is_last) || true
                else
                  c0, v0 = conditions[-2]
                  c1, v1 = conditions[-1]

                  filter = ineq(c1, v1, eq: !is_last)
                  list = filter ? [Sequel.&({c0.name => v0}, filter)] : [{c0.name => v0}]

                  conditions[0..-2].each do |c, v|
                    if filter = ineq(c, v, eq: false)
                      list << filter
                    end
                  end
                  Sequel.|(*list)
                end
              }
            )
          )
        end

        private

        def ineq(column, value, eq: true)
          ascending = column.direction == :asc
          nulls_upcoming = column.nulls == :last
          value_is_null = value.nil?

          method = "#{ascending ? '>' : '<'}#{'=' if eq}"

          if nulls_upcoming
            if value_is_null
              if eq
                {column.name => nil}
              end
            else
              Sequel.|(Sequel.virtual_row{|o| o.__send__(method, column.name, value)}, {column.name => nil})
            end
          else
            if value_is_null
              if !eq
                Sequel.~({column.name => nil})
              end
            else
              Sequel.virtual_row { |o| o.__send__(method, column.name, value) }
            end
          end
        end
      end
    end
  end

  Dataset.register_extension(:seek_pagination, SeekPagination)
end
