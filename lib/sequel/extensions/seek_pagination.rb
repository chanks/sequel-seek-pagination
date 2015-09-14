require 'sequel'
require 'sequel/extensions/seek_pagination/version'

module Sequel
  module SeekPagination
    class Error < StandardError; end

    def seek_paginate(count, from: nil, after: nil, not_null: [])
      order = opts[:order]

      if order.nil? || order.length.zero?
        raise Error, "cannot seek paginate on a dataset with no order"
      elsif after && from
        raise Error, "cannot pass both :after and :from params to seek_paginate"
      end

      ds = limit(count)

      if from
        OrderedColumn.apply(ds, order.zip([*from]), include_value: true, not_null: not_null)
      elsif after
        OrderedColumn.apply(ds, order.zip([*after]), not_null: not_null)
      else
        ds
      end
    end

    private

    class OrderedColumn
      attr_reader :name, :direction, :nulls, :value, :not_null

      def initialize(order, value, not_null:)
        @value = value
        @not_null = not_null
        @name, @direction, @nulls =
          case order
          when Symbol
            [order, :asc, :last]
          when Sequel::SQL::OrderedExpression
            direction = order.descending ? :desc : :asc
            nulls = order.nulls || default_nulls_option_for_direction(direction)
            [order.expression, direction, nulls]
          else
            raise "Unrecognized order!: #{order.inspect}"
          end
      end

      def default_nulls_option_for_direction(direction)
        case direction
        when :asc  then :last
        when :desc then :first
        else raise "Bad direction: #{direction.inspect}"
        end
      end

      def nulls_option_is_default?
        nulls == default_nulls_option_for_direction(direction)
      end

      def eq_filter
        {name => value}
      end

      def null_filter
        {name => nil}
      end

      def ineq(eq: true)
        nulls_upcoming = !@not_null && nulls == :last

        if !value.nil?
          method = "#{direction == :asc ? '>' : '<'}#{'=' if eq}"
          filter = Sequel.virtual_row{|o| o.__send__(method, name, value)}
          nulls_upcoming ? Sequel.|(filter, null_filter) : filter
        else
          if nulls_upcoming && eq
            null_filter
          elsif !nulls_upcoming && !eq
            Sequel.~(null_filter)
          end
        end
      end

      class << self
        def apply(dataset, order_sets, include_value: false, not_null:)
          orders = order_sets.map do |order, value|
            column = case order
                     when Symbol then order
                     when Sequel::SQL::OrderedExpression then order.expression
                     else raise "Bad! #{order}"
                     end

            new(order, value, not_null: not_null.include?(column))
          end

          length = orders.length

          # Special-case the common case where we can do WHERE (non_nullable_1, non_nullable_2) > (1, 2)
          if length > 1 && orders.map(&:direction).uniq.length == 1 && orders.all? { |o| o.not_null && o.nulls_option_is_default? }
            method = orders.first.direction == :asc ? '>' : '<'
            method << '='.freeze if include_value
            return dataset.where{|o| o.__send__(method, orders.map(&:name), orders.map(&:value))}
          end

          dataset.where(
            Sequel.&(
              *length.times.map { |i|
                is_last = i == length - 1
                conditions = orders[0..i]

                if i.zero?
                  conditions[0].ineq(eq: (include_value || !is_last))
                else
                  c = conditions[-2]

                  list = if filter = conditions[-1].ineq(eq: (include_value || !is_last))
                           [Sequel.&(c.eq_filter, filter)]
                         else
                           [c.eq_filter]
                         end

                  list += conditions[0..-2].map { |c| c.ineq(eq: false) }

                  Sequel.|(*list.compact)
                end
              }.compact
            )
          )
        end
      end
    end
  end

  Dataset.register_extension(:seek_pagination, SeekPagination)
end
