require 'sequel'
require 'sequel/extensions/seek_pagination/version'

module Sequel
  module SeekPagination
    class Error < StandardError; end

    def seek_paginate(count, from: nil, after: nil, from_pk: nil, after_pk: nil, not_null: nil)
      order = opts[:order]
      model = @model

      if order.nil? || order.length.zero?
        raise Error, "cannot seek_paginate on a dataset with no order"
      elsif from && after
        raise Error, "cannot pass both :from and :after params to seek_paginate"
      elsif model.nil? && (from_pk || after_pk)
        raise Error, "passed the :#{from_pk ? 'from' : 'after'}_pk option to seek_paginate on a dataset that doesn't have an associated model"
      end

      ds = limit(count)

      if values = from || after
        values = Array(values)

        if values.length != order.length
          raise Error, "passed the wrong number of values in the :#{from ? 'from' : 'after'} option to seek_paginate"
        end

        if not_null.nil?
          not_null = []

          # If the dataset was chained off a model, use its stored schema
          # information to figure out what columns are not null.
          if model
            model.db_schema.each do |column, schema|
              not_null << column if schema[:allow_null] == false
            end
          end
        end

        # If we're paginating with a :from value, we want to include the row
        # that has those exact values.
        OrderedColumnSet.new(order.zip(values), include_exact_match: !!from, not_null: not_null).apply(ds)
      else
        ds
      end
    end

    private

    class OrderedColumnSet
      attr_reader :not_null, :include_exact_match, :orders

      def initialize(order_values, include_exact_match:, not_null:)
        @not_null = not_null
        @include_exact_match = include_exact_match
        @orders = order_values.map { |order, value| OrderedColumn.new(self, order, value) }
      end

      def apply(dataset)
        length = orders.length

        conditions =
          # Handle the common case where we can do a simpler (and faster)
          # WHERE (non_nullable_1, non_nullable_2) > (1, 2) clause.
          if length > 1 && orders.all?(&:not_null) && has_uniform_order_direction?
            method = orders.first.direction == :asc ? '>' : '<'
            method << '='.freeze if include_exact_match
            Sequel.virtual_row{|o| o.__send__(method, orders.map(&:name), orders.map(&:value))}
          else
            Sequel.&(
              *length.times.map { |i|
                allow_equal = include_exact_match || i != length - 1
                conditions = orders[0..i]

                if i.zero?
                  conditions[0].ineq(eq: allow_equal)
                else
                  c = conditions[-2]

                  list = if filter = conditions[-1].ineq(eq: allow_equal)
                           [Sequel.&(c.eq_filter, filter)]
                         else
                           [c.eq_filter]
                         end

                  list += conditions[0..-2].map { |c| c.ineq(eq: false) }

                  Sequel.|(*list.compact)
                end
              }.compact
            )
          end

        dataset.where(conditions)
      end

      private

      def has_uniform_order_direction?
        direction = nil
        orders.each do |order|
          direction ||= order.direction
          return false unless direction == order.direction
        end
        true
      end
    end

    class OrderedColumn
      attr_reader :name, :direction, :nulls, :value, :not_null

      def initialize(set, order, value)
        @set = set
        @value = value
        @name, @direction, @nulls =
          case order
          when Symbol
            [order, :asc, :last]
          when Sequel::SQL::OrderedExpression
            direction = order.descending ? :desc : :asc
            nulls = order.nulls || default_nulls_option(direction)
            [order.expression, direction, nulls]
          else
            raise "Unrecognized order!: #{order.inspect}"
          end

        @not_null = set.not_null.include?(@name)
      end

      def nulls_option_is_default?
        nulls == default_nulls_option(direction)
      end

      def eq_filter
        {name => value}
      end

      def null_filter
        {name => nil}
      end

      def ineq(eq: true)
        nulls_upcoming = !not_null && nulls == :last

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

      private

      # By default, Postgres sorts NULLs as higher than any other value. So we
      # can treat a plain column ASC as column ASC NULLS LAST, and a plain
      # column DESC as column DESC NULLS FIRST.
      def default_nulls_option(direction)
        case direction
        when :asc  then :last
        when :desc then :first
        else raise "Bad direction: #{direction.inspect}"
        end
      end
    end
  end

  Dataset.register_extension(:seek_pagination, SeekPagination)
end
