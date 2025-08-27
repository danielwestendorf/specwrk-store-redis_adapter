# frozen_string_literal: true

RSpec.describe Specwrk::Store::RedisAdapter do
  let(:uri) { "redis://localhost:6327" }
  let(:connection_pool_dbl) { instance_double(RedisClient::Pooled) }
  let(:redis_client_dbl) { instance_double(RedisClient) }
  let(:redlock_client_dbl) { instance_double(Redlock::Client) }

  before { described_class.reset_connections! }

  describe ".with_lock" do
    let(:key) { "foobar" }

    it "locks and yields" do
      expect(described_class).to receive(:connection_pool_for)
        .with(uri)
        .and_return(connection_pool_dbl)

      expect(connection_pool_dbl).to receive(:with)
        .and_yield(redis_client_dbl)

      id = "uuid-123"
      expect(SecureRandom).to receive(:uuid)
        .and_return(id)

      expect(redis_client_dbl).to receive(:pipelined) do |&blk|
        blk.call(double("pipeline", call: true))
      end.twice

      expect(redis_client_dbl).to receive(:call)
        .with("LINDEX", instance_of(String), 0)
        .and_return(id)

      foo = 1
      described_class.with_lock(uri, key) do
        foo += 1
      end

      expect(foo).to eq(2)
    end
  end

  describe ".connection_pool_for" do
    it "maintains connection pools" do
      expect(described_class.connection_pool_for(uri).object_id).to eq(described_class.connection_pool_for(uri).object_id)
    end
  end

  describe "with_connection" do
    let(:scope) { "foobar" }
    let(:instance) { described_class.new(uri, scope) }

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
          .with("HGET", scope, "foo")
          .and_return(JSON.generate({a: 1}))
      end

      it { is_expected.to eq(a: 1) }
    end

    describe "#[]=" do
      subject { instance["foo"] = {a: 1} }

      before do
        allow(redis_client_dbl).to receive(:call)
          .with("HSET", scope, "foo", JSON.generate({a: 1}))
          .and_return("fizzbuzz")
      end

      it { is_expected.to eq(a: 1) }
    end

    describe "#keys" do
      subject { instance.keys }

      before do
        allow(redis_client_dbl).to receive(:call)
          .with("HKEYS", scope)
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
          .with("DEL", scope)

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
            .with("HDEL", scope, 1, 2, 3, 4)
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
              "HMSET", scope, :a, JSON.generate({a: 1}), :b, JSON.generate({b: 2})
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
              "HMGET", scope, "a", "b", "c"
            ).and_return([
              JSON.generate({x: 1}),
              nil,
              JSON.generate({z: 3})
            ])
        end

        it { is_expected.to eq("a" => {x: 1}, "c" => {z: 3}) }
      end

      context "when all values are nil" do
        let(:read_keys) { %w[x y] }

        before do
          allow(redis_client_dbl).to receive(:call)
            .with(
              "HMGET", scope, "x", "y"
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
            "HLEN", scope
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
  end
end
