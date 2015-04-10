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

      orders     = order.map { |o| parse_column_and_direction(o) }
      columns    = orders.map{|o| o[0]}
      directions = orders.map{|o| o[1]}.uniq

      unless directions.length == 1
        raise Error, "cannot seek paginate on a query ordering by multiple columns in different directions"
      end

      ds = limit(count)

      if after
        case directions.first
        when :asc  then ds.where{|r| r.>(columns, after)}
        when :desc then ds.where{|r| r.<(columns, after)}
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
