# frozen_string_literal: true

RSpec.describe Specwrk::Store::RedisAdapter do
  let(:uri) { "redis://localhost:6327" }
  let(:connection_pool_dbl) { instance_double(RedisClient::Pooled) }
  let(:redis_client_dbl) { instance_double(RedisClient) }

  before { described_class.reset_connections! }

  describe ".with_lock" do
    let(:key) { "foobar" }
    let(:lock_id) { "uuid-123" }

    before do
      expect(described_class).to receive(:connection_pool_for)
        .with(uri)
        .and_return(connection_pool_dbl)

      expect(connection_pool_dbl).to receive(:with)
        .and_yield(redis_client_dbl)

      expect(SecureRandom).to receive(:uuid)
        .and_return(lock_id)
    end

    it "locks and yields" do
      expect(redis_client_dbl).to receive(:call)
        .with(
          "EVALSHA",
          described_class::LOCK_SCRIPT_SHA,
          1,
          "specwrk-lock-#{key}",
          lock_id,
          described_class::LOCK_TTL_MILLISECONDS
        ).and_return(1)

      expect(redis_client_dbl).to receive(:call)
        .with("EVALSHA", described_class::UNLOCK_SCRIPT_SHA, 1, "specwrk-lock-#{key}", lock_id)
        .and_return(1)

      foo = 1
      described_class.with_lock(uri, key) do
        foo += 1
      end

      expect(foo).to eq(2)
    end

    it "raises when the lock is unavailable" do
      expect(redis_client_dbl).to receive(:call)
        .with(
          "EVALSHA",
          described_class::LOCK_SCRIPT_SHA,
          1,
          "specwrk-lock-#{key}",
          lock_id,
          described_class::LOCK_TTL_MILLISECONDS
        ).and_return(0)

      expect(described_class).not_to receive(:sleep)
      expect(redis_client_dbl).not_to receive(:call)
        .with("EVALSHA", described_class::UNLOCK_SCRIPT_SHA, 1, "specwrk-lock-#{key}", lock_id)

      yielded = false

      expect do
        described_class.with_lock(uri, key) { yielded = true }
      end.to raise_error(Specwrk::Store::LockUnavailableError)

      expect(yielded).to eq(false)
    end

    it "unlocks when the block raises" do
      allow(redis_client_dbl).to receive(:call)
        .with(
          "EVALSHA",
          described_class::LOCK_SCRIPT_SHA,
          1,
          "specwrk-lock-#{key}",
          lock_id,
          described_class::LOCK_TTL_MILLISECONDS
        ).and_return(1)

      expect(redis_client_dbl).to receive(:call)
        .with("EVALSHA", described_class::UNLOCK_SCRIPT_SHA, 1, "specwrk-lock-#{key}", lock_id)
        .and_return(1)

      expect do
        described_class.with_lock(uri, key) { raise "boom" }
      end.to raise_error("boom")
    end

    it "does not unlock when acquisition fails" do
      expect(redis_client_dbl).to receive(:call)
        .with(
          "EVALSHA",
          described_class::LOCK_SCRIPT_SHA,
          1,
          "specwrk-lock-#{key}",
          lock_id,
          described_class::LOCK_TTL_MILLISECONDS
        ).and_raise("connection failed")

      expect do
        described_class.with_lock(uri, key) { true }
      end.to raise_error("connection failed")
    end

    it "loads a missing script and retries it by its content version" do
      lock_arguments = [
        "EVALSHA",
        described_class::LOCK_SCRIPT_SHA,
        1,
        "specwrk-lock-#{key}",
        lock_id,
        described_class::LOCK_TTL_MILLISECONDS
      ]

      expect(redis_client_dbl).to receive(:call)
        .with(*lock_arguments)
        .and_raise(RedisClient::NoScriptError, "NOSCRIPT script missing")
        .ordered
      expect(redis_client_dbl).to receive(:call)
        .with("SCRIPT", "LOAD", described_class::LOCK_SCRIPT)
        .and_return(described_class::LOCK_SCRIPT_SHA)
        .ordered
      expect(redis_client_dbl).to receive(:call)
        .with(*lock_arguments)
        .and_return(1)
        .ordered
      expect(redis_client_dbl).to receive(:call)
        .with("EVALSHA", described_class::UNLOCK_SCRIPT_SHA, 1, "specwrk-lock-#{key}", lock_id)
        .and_return(1)
        .ordered

      described_class.with_lock(uri, key) { true }
    end
  end

  describe ".connection_pool_for" do
    it "maintains connection pools" do
      expect(described_class.connection_pool_for(uri).object_id).to eq(described_class.connection_pool_for(uri).object_id)
    end
  end

  describe "with_connection" do
    let(:scope) { "foobar" }
    let(:adapter_name) { (described_class.serializer.adapter_name == "json") ? nil : described_class.serializer.adapter_name }
    let(:serializer_scope) { [adapter_name, scope].compact.join("-") }
    let(:instance) { described_class.new(uri, scope) }
    let(:serializer) { described_class.serializer }

    before do
      allow(described_class).to receive(:connection_pool_for)
        .with(uri)
        .and_return(connection_pool_dbl)

      allow(connection_pool_dbl).to receive(:with)
        .and_yield(redis_client_dbl)
    end

    describe "#[]" do
      subject { instance["foo"] }

      before do
        allow(redis_client_dbl).to receive(:call)
          .with("HGET", serializer_scope, "foo")
          .and_return(serializer.dump({a: 1}))
      end

      it { is_expected.to eq(a: 1) }
    end

    describe "#[]=" do
      subject { instance["foo"] = {a: 1} }

      before do
        allow(redis_client_dbl).to receive(:call)
          .with("HSET", serializer_scope, "foo", serializer.dump({a: 1}))
          .and_return("fizzbuzz")
      end

      it { is_expected.to eq(a: 1) }
    end

    describe "#keys" do
      subject { instance.keys }

      before do
        allow(redis_client_dbl).to receive(:call)
          .with("HKEYS", serializer_scope)
          .and_return(keys)
      end

      context "when keys exist across multiple scan batches" do
        let(:keys) { %w[c a b] }

        it { is_expected.to match_array(%w[a b c]) }
      end

      context "when there are no keys" do
        let(:keys) { [] }

        it { is_expected.to eq([]) }
      end
    end

    describe "#clear" do
      subject { instance.clear }

      it "deletes all keys" do
        expect(redis_client_dbl).to receive(:call)
          .with("DEL", serializer_scope)

        instance.clear
      end
    end

    describe "#delete" do
      subject { instance.delete(*keys) }

      context "no keys" do
        let(:keys) {}

        it { is_expected.to eq(nil) }
      end

      context "some keys" do
        let(:keys) { [1, 2, 3, 4] }

        before do
          allow(redis_client_dbl).to receive(:call)
            .with("HDEL", serializer_scope, 1, 2, 3, 4)
            .and_return("foobar")
        end

        it { is_expected.to eq("foobar") }
      end
    end

    describe "#merge! and #multi_write" do
      subject { instance.merge!(h2) }

      context "when the hash has entries" do
        let(:h2) do
          {
            a: {a: 1},
            b: {b: 2}
          }
        end

        before do
          allow(redis_client_dbl).to receive(:call)
            .with(
              "HMSET", serializer_scope, :a, serializer.dump({a: 1}), :b, serializer.dump({b: 2})
            ).and_return("foobar")
        end

        it { is_expected.to eq("foobar") }
      end

      context "when the hash is empty" do
        let(:h2) { {} }

        it { is_expected.to eq(nil) }
      end

      context "when the hash is nil" do
        let(:h2) { nil }

        it { is_expected.to eq(nil) }
      end
    end

    describe "#multi_read" do
      subject { instance.multi_read(*read_keys) }

      context "when no keys are provided" do
        let(:read_keys) { [] }

        it { is_expected.to eq({}) }
      end

      context "when some keys exist and others do not" do
        let(:read_keys) { %w[a b c] }

        before do
          allow(redis_client_dbl).to receive(:call)
            .with(
              "HMGET", serializer_scope, "a", "b", "c"
            ).and_return([
              serializer.dump({x: 1}),
              nil,
              serializer.dump({z: 3})
            ])
        end

        it { is_expected.to eq("a" => {x: 1}, "c" => {z: 3}) }
      end

      context "when all values are nil" do
        let(:read_keys) { %w[x y] }

        before do
          allow(redis_client_dbl).to receive(:call)
            .with(
              "HMGET", serializer_scope, "x", "y"
            ).and_return([nil, nil])
        end

        it { is_expected.to eq({}) }
      end
    end

    describe "#empty?" do
      subject { instance.empty? }

      before do
        allow(redis_client_dbl).to receive(:call)
          .with(
            "HLEN", serializer_scope
          ).and_return(keys.length)
      end

      context "no keys" do
        let(:keys) { [] }

        it { is_expected.to eq(true) }
      end

      context "keys" do
        let(:keys) { [1] }

        it { is_expected.to eq(false) }
      end
    end

    describe "scope serialization" do
      subject { instance.send(:scope) }

      let(:instance) { described_class.new(uri, scope) }

      around do |example|
        original = ENV["SPECWRK_STORE_SERIALIZER"]
        ENV["SPECWRK_STORE_SERIALIZER"] = serializer
        described_class.reset_serializer!
        example.run
        ENV["SPECWRK_STORE_SERIALIZER"] = original
        described_class.reset_serializer!
      end

      context "when using the default json serializer" do
        let(:serializer) { "json" }

        it { is_expected.to eq(scope) }
      end

      context "when SPECWRK_STORE_SERIALIZER is set to msgpack" do
        let(:serializer) { "msgpack" }

        it { is_expected.to eq(serializer_scope) }
      end
    end
  end
end
