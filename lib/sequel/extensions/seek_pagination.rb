require 'sequel'
require 'sequel/extensions/seek_pagination/version'

module Sequel
  module SeekPagination
    class Error < StandardError; end

    def seek(missing_pk: :raise, **args)
      if c = seek_conditions(raise_on_missing_pk: missing_pk == :raise, **args)
        where(c)
      else
        case missing_pk
        when :ignore     then self
        when :nullify    then nullify
        when :return_nil then nil
        else raise Error, "passed an invalid argument for missing_pk: #{missing_pk.inspect}"
        end
      end
    end

    def seek_conditions(
      value: nil,
      pk: nil,
      include_exact_match: false,
      not_null: nil,
      raise_on_missing_pk: false
    )

      order = opts[:order]
      model = opts[:model]

      if !(value.nil? ^ pk.nil?)
        raise Error, "must pass exactly one of :value and :pk to #seek"
      elsif order.nil? || order.length.zero?
        raise Error, "cannot call #seek on a dataset with no order"
      end

      values =
        if pk
          get_order_values_for_pk(pk, raise_on_failure: raise_on_missing_pk)
        else
          Array(value)
        end

      return unless values

      if values.length != order.length
        raise Error, "passed the wrong number of values to #seek"
      end

      if not_null.nil?
        not_null = []

        # If the dataset was chained off a model, use its stored schema
        # information to figure out what columns are not null.
        if model
          table = model.table_name

          model.db_schema.each do |column, schema|
            next if schema[:allow_null]
            not_null << column << Sequel.qualify(table, column)
          end
        end
      end

      OrderedColumnSet.new(
        order.zip(values),
        include_exact_match: include_exact_match,
        not_null: not_null
      ).build_conditions
    end

    def get_order_values_for_pk(pk, raise_on_failure: false)
      order = opts[:order]

      unless model = opts[:model]
        raise Error, "attempted a primary key lookup on a dataset that doesn't have an associated model"
      end

      al = nil
      aliases = order.map { al = al ? al.next : :a }

      ds =
        cached_dataset(:_seek_pagination_get_order_values_ds) do
          # Need to load the values to order from for that pk from the DB, so we
          # need to fetch the actual expressions being ordered by. Also,
          # Dataset#get won't like it if we pass it expressions that aren't
          # simple columns, so we need to give it aliases for everything.
          naked.limit(1).select(
            *order.map.with_index { |o, i|
              expression = Sequel::SQL::OrderedExpression === o ? o.expression : o
              Sequel.as(expression, aliases[i])
            }
          )
        end

      condition = model.qualified_primary_key_hash(pk)

      if result = ds.where_all(condition).first
        result.values_at(*aliases)
      elsif raise_on_failure
        raise NoMatchingRow.new(ds.where(condition))
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

      def build_conditions
        length = orders.length

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
              allow_equal = include_exact_match || i != (length - 1)
              conditions = orders[0..i]

              if i.zero?
                conditions[0].inequality_condition(allow_equal: allow_equal)
              else
                c = conditions[-2]

                list = if filter = conditions[-1].inequality_condition(allow_equal: allow_equal)
                         [Sequel.&(c.eq_filter, filter)]
                       else
                         [c.eq_filter]
                       end

                list += conditions[0..-2].map { |c| c.inequality_condition(allow_equal: false) }

                Sequel.|(*list.compact)
              end
            }.compact
          )
        end
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

      def inequality_condition(allow_equal:)
        nulls_upcoming = !not_null && nulls == :last

        if value.nil?
          if nulls_upcoming && allow_equal
            null_filter
          elsif !nulls_upcoming && !allow_equal
            Sequel.~(null_filter)
          else
            # No condition necessary.
            nil
          end
        else
          # Value is not null.
          filter = Sequel.virtual_row { |o| o.__send__(inequality_method(allow_equal), name, value) }

          if nulls_upcoming
            Sequel.|(filter, null_filter)
          else
            filter
          end
        end
      end

      def inequality_method(allow_equal)
        case direction
        when :asc  then allow_equal ? :>= : :>
        when :desc then allow_equal ? :<= : :<
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
