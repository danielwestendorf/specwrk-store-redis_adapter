# frozen_string_literal: true

require "json"

require "specwrk/store/base_adapter"
require "redis-client"
require "redlock"

module Specwrk
  class Store
    class RedisAdapter < Specwrk::Store::BaseAdapter
      VERSION = "0.1.0"

      REDIS_KEY_DELIMITER = "||||"

      @connection_pools = {}
      @mutex = Mutex.new

      class << self
        def with_lock(uri, key)
          connection_pool_for(uri).with do |connection|
            client = Redlock::Client.new(
              [connection],
              retry_count: lock_retry_count,
              retry_delay: lock_retry_delay,
              retry_jitter: lock_retry_jitter
            )

            until (lock = client.lock("specwrk-lock-#{key}", lock_ttl))
              sleep(rand(0.001..0.09))
            end

            yield

            client.unlock(lock)
          end
        end

        def connection_pool_for(uri)
          return @connection_pools[uri] if @connection_pools.key? uri

          @mutex.synchronize do
            @connection_pools[uri] ||= RedisClient.config(url: uri).new_pool(
              size: ENV.fetch("SPECWRK_THREAD_COUNT", "4").to_i
            )
          end
        end

        def reset_connections!
          @connection_pools.clear
        end

        private

        # In ms
        def lock_ttl
          ENV.fetch("SPECWRK_REDIS_ADAPTER_LOCK_TTL", "5000").to_i
        end

        # See https://www.rubydoc.info/gems/redlock/Redlock/Client#initialize-instance_method
        def lock_retry_count
          ENV.fetch("SPECWRK_REDIS_ADAPTER_LOCK_RETRY_COUNT", "3").to_i
        end

        def lock_retry_delay
          ENV.fetch("SPECWRK_REDIS_ADAPTER_LOCK_RETRY_DELAY", "100").to_i
        end

        def lock_retry_jitter
          ENV.fetch("SPECWRK_REDIS_ADAPTER_LOCK_RETRY_JITTER", "10").to_i
        end
      end

      def [](key)
        with_connection do |redis|
          value = redis.call("GET", encode_key(key))
          JSON.parse(value, symbolize_names: true) if value
        end
      end

      def []=(key, value)
        with_connection do |redis|
          redis.call("SET", encode_key(key), JSON.generate(value))
        end
      end

      def keys
        [].tap do |collected|
          scan_for("#{scope}#{REDIS_KEY_DELIMITER}*") { |k| collected << decode_key(k) }
        end
      end

      def clear
        delete(*keys)
      end

      def delete(*keys)
        return if keys.length.zero?

        with_connection do |redis|
          redis.call("DEL", *keys.map { |key| encode_key key })
        end
      end

      def merge!(h2)
        multi_write(h2)
      end

      def multi_read(*read_keys)
        return {} if read_keys.length.zero?

        values = with_connection do |redis|
          redis.call("MGET", *read_keys.map { |key| encode_key(key) })
        end

        result = {}

        read_keys.zip(values).each do |key, value|
          next if value.nil?
          result[key] = JSON.parse(value, symbolize_names: true)
        end

        result
      end

      def multi_write(hash)
        return if hash.nil? || hash.length.zero?

        with_connection do |redis|
          redis.call("MSET", *hash.flat_map { |key, value| [encode_key(key), JSON.generate(value)] })
        end
      end

      def empty?
        keys.length.zero?
      end

      private

      def with_connection
        self.class.connection_pool_for(uri).with do |connection|
          yield connection
        end
      end

      def encode_key(key)
        [scope, REDIS_KEY_DELIMITER, key].join
      end

      def decode_key(key)
        key.split(REDIS_KEY_DELIMITER).last
      end

      def scan_for(match)
        with_connection do |redis|
          cursor = "0"
          loop do
            cursor, batch = redis.call("SCAN", cursor, "MATCH", match, "COUNT", 5_000)
            batch.each { |k| yield k }
            break if cursor == "0"
          end
        end
      end
    end
  end
end
