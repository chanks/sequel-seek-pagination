require 'sequel'
require 'sequel/extensions/seek_pagination/version'

module Sequel
  module SeekPagination
    class Error < StandardError; end

    def seek(value: nil, pk: nil, include_exact_match: false, not_null: nil)
      order = opts[:order]
      model = @model

      if !(value.nil? ^ pk.nil?)
        raise Error, "must pass exactly one of :value and :pk to #seek"
      elsif order.nil? || order.length.zero?
        raise Error, "cannot seek on a dataset with no order"
      elsif model.nil? && pk
        raise Error, "attempted a pk lookup on a dataset that doesn't have an associated model"
      end

      if pk
        target_ds = where(model.qualified_primary_key_hash(pk))

        # Need to load the values to order from for that pk from the DB, so we
        # need to fetch the actual expressions being ordered by. Also,
        # Dataset#get won't like it if we pass it expressions that aren't
        # simple columns, so we need to give it aliases for everything.
        al = :a
        gettable = order.map do |o|
          expression = Sequel::SQL::OrderedExpression === o ? o.expression : o
          Sequel.as(expression, (al = al.next))
        end

        unless values = target_ds.get(gettable)
          raise NoMatchingRow.new(target_ds)
        end
      else
        values = Array(value)

        if values.length != order.length
          raise Error, "passed the wrong number of values to #seek"
        end
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

      OrderedColumnSet.new(order.zip(values), include_exact_match: include_exact_match, not_null: not_null).apply(self)
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
            Sequel.virtual_row do |o|
              o.__send__(
                orders.first.inequality_method(include_exact_match),
                orders.map(&:name),
                orders.map(&:value)
              )
            end
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
          when Sequel::SQL::OrderedExpression
            direction = order.descending ? :desc : :asc
            nulls = order.nulls || default_nulls_option(direction)
            [order.expression, direction, nulls]
          else
            [order, :asc, :last]
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

      def ineq(eq:)
        nulls_upcoming = !not_null && nulls == :last

        if !value.nil?
          filter = Sequel.virtual_row{|o| o.__send__(inequality_method(eq), name, value)}
          nulls_upcoming ? Sequel.|(filter, null_filter) : filter
        else
          if nulls_upcoming && eq
            null_filter
          elsif !nulls_upcoming && !eq
            Sequel.~(null_filter)
          end
        end
      end

      def inequality_method(eq)
        case direction
        when :asc  then eq ? :>= : :>
        when :desc then eq ? :<= : :<
        else raise "Bad direction: #{direction.inspect}"
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
