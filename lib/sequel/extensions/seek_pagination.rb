require 'sequel'
require 'sequel/extensions/seek_pagination/version'

module Sequel
  module SeekPagination
    def seek_paginate(count, after: nil)
      ds = limit(count)

      if after
        column, direction = parse_column_and_direction(opts[:order].first)

        case direction
        when :asc  then ds.where{|r| r.>(r.__send__(column), after)}
        when :desc then ds.where{|r| r.<(r.__send__(column), after)}
        else raise "Bad direction!: #{direction.inspect}"
        end
      else
        ds
      end
    end

    private

    def parse_column_and_direction(order)
      case order
      when Symbol                         then [order, :asc]
      when Sequel::SQL::OrderedExpression then [order.expression, order.descending ? :desc : :asc]
      else raise "Unrecognized order!: #{order.inspect}"
      end
    end
  end

  Dataset.register_extension(:seek_pagination, SeekPagination)
end
