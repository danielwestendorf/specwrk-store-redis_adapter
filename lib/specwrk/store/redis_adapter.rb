# frozen_string_literal: true

require "json"
require "securerandom"

require "specwrk/store/base_adapter"
require "specwrk/store/serializer"
require "redis-client"

module Specwrk
  class Store
    class RedisAdapter < Specwrk::Store::BaseAdapter
      @connection_pools = {}
      @mutex = Mutex.new

      class << self
        def with_lock(uri, key)
          connection_pool_for(uri).with do |connection|
            Thread.current[:connection] = connection

            id = SecureRandom.uuid
            queue = "specwrk-lock-#{key}"
            connection.pipelined do |pipeline|
              pipeline.call("RPUSH", queue, id)
              pipeline.call("EXPIRE", queue, 10) # only set if no expireat already
            end

            # wait for our id to be first in line or the queue to expire
            sleep(0.01 * rand) until [id, nil].include? connection.call("LINDEX", queue, 0)

            yield
          ensure
            connection.pipelined do |pipeline|
              pipeline.call("LPOP", queue)
              pipeline.call("EXPIRE", queue, 10) # keeps the queue fresh when things are moving
            end

            Thread.current[:connection] = nil
          end
        end

        def connection_pool_for(uri)
          return @connection_pools[uri] if @connection_pools.key? uri

          @mutex.synchronize do
            @connection_pools[uri] ||= RedisClient.config(url: uri).new_pool(
              size: ENV.fetch("SPECWRK_THREAD_COUNT", ENV.fetch("SPECWRK_COUNT", "4")).to_i
            )
          end
        end

        def reset_connections!
          @connection_pools.clear
        end
      end

      def [](key)
        with_connection do |redis|
          value = redis.call("HGET", scope, key)
          self.class.serializer.load(value) if value
        end
      end

      def []=(key, value)
        with_connection do |redis|
          redis.call("HSET", scope, key, self.class.serializer.dump(value))
        end
      end

      def keys
        with_connection do |redis|
          redis.call("HKEYS", scope)
        end
      end

      def clear
        with_connection do |redis|
          redis.call("DEL", scope)
        end
      end

      def delete(*keys)
        return if keys.length.zero?

        with_connection do |redis|
          redis.call("HDEL", scope, *keys)
        end
      end

      def merge!(h2)
        multi_write(h2)
      end

      def multi_read(*read_keys)
        return {} if read_keys.length.zero?

        values = with_connection do |redis|
          redis.call("HMGET", scope, *read_keys)
        end

        result = {}

        read_keys.zip(values).each do |key, value|
          next if value.nil?
          result[key] = self.class.serializer.load(value)
        end

        result
      end

      def multi_write(hash)
        return if hash.nil? || hash.length.zero?

        with_connection do |redis|
          redis.call("HMSET", scope, *hash.flat_map { |key, value| [key, self.class.serializer.dump(value)] })
        end
      end

      def empty?
        with_connection do |redis|
          redis.call("HLEN", scope).zero?
        end
      end

      private

      def with_connection
        if Thread.current[:connection]
          yield Thread.current[:connection]
        else
          self.class.connection_pool_for(uri).with do |connection|
            yield connection
          end
        end
      end

      # override
      def scope
        return @scope if self.class.serializer.adapter_name == "json"

        [self.class.serializer.adapter_name, @scope].join("-")
      end
    end
  end
end
