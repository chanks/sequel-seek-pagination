require 'sequel'
require 'sequel/extensions/seek_pagination/version'

module Sequel
  class Dataset
    module SeekPagination
      def seek_paginate(count)
        limit(count)
      end
    end

    register_extension(:seek_pagination, SeekPagination)
  end
end
