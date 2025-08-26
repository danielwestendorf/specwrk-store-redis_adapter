# frozen_string_literal: true

RSpec.describe Specwrk::Store::RedisAdapter do
  def encoded_key(key)
    "#{scope}#{described_class::REDIS_KEY_DELIMITER}#{key}"
  end

  let(:uri) { "redis://localhost:6327" }
  let(:connection_pool_dbl) { instance_double(RedisClient::Pooled) }
  let(:redis_client_dbl) { instance_double(RedisClient) }
  let(:redlock_client_dbl) { instance_double(Redlock::Client) }

  before { described_class.reset_connections! }

  describe ".with_lock" do
    let(:key) { "foobar" }
    let(:env) do
      {
        "SPECWRK_REDIS_ADAPTER_LOCK_TTL" => "1",
        "SPECWRK_REDIS_ADAPTER_LOCK_RETRY_COUNT" => "42",
        "SPECWRK_REDIS_ADAPTER_LOCK_RETRY_DELAY" => "43",
        "SPECWRK_REDIS_ADAPTER_LOCK_RETRY_JITTER" => "44"
      }
    end

    it "locks and yields" do
      stub_const("ENV", env)

      expect(described_class).to receive(:connection_pool_for)
        .with(uri)
        .and_return(connection_pool_dbl)

      expect(connection_pool_dbl).to receive(:with)
        .and_yield(redis_client_dbl)

      expect(Redlock::Client).to receive(:new)
        .with(
          [redis_client_dbl],
          retry_count: 42,
          retry_delay: 43,
          retry_jitter: 44
        ).and_return(redlock_client_dbl)

      expect(redlock_client_dbl).to receive(:lock)
        .with("specwrk-lock-foobar", 1)
        .and_return(false)

      expect(described_class).to receive(:sleep)
        .and_return(true)

      expect(redlock_client_dbl).to receive(:lock)
        .with("specwrk-lock-foobar", 1)
        .and_return("the-lock-info")

      expect(redlock_client_dbl).to receive(:unlock)
        .with("the-lock-info")

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
          .with("GET", "#{scope}#{described_class::REDIS_KEY_DELIMITER}foo")
          .and_return(JSON.generate({a: 1}))
      end

      it { is_expected.to eq(a: 1) }
    end

    describe "#[]=" do
      subject { instance["foo"] = {a: 1} }

      before do
        allow(redis_client_dbl).to receive(:call)
          .with("SET", encoded_key("foo"), JSON.generate({a: 1}))
          .and_return("fizzbuzz")
      end

      it { is_expected.to eq(a: 1) }
    end

    describe "#keys" do
      subject { instance.keys }

      let(:match) { "#{scope}#{described_class::REDIS_KEY_DELIMITER}*" }

      context "when keys exist across multiple scan batches" do
        before do
          expect(redis_client_dbl).to receive(:call)
            .with("SCAN", "0", "MATCH", match, "COUNT", 5_000)
            .and_return(["1", [encoded_key("a"), encoded_key("b")]])

          expect(redis_client_dbl).to receive(:call)
            .with("SCAN", "1", "MATCH", match, "COUNT", 5_000)
            .and_return(["0", [encoded_key("c")]])
        end

        it { is_expected.to eq(%w[a b c]) }
      end

      context "when there are no keys" do
        before do
          expect(redis_client_dbl).to receive(:call)
            .with("SCAN", "0", "MATCH", match, "COUNT", 5_000)
            .and_return(["0", []])
        end

        it { is_expected.to eq([]) }
      end
    end

    describe "#clear" do
      subject { instance.clear }

      it "deletes all keys" do
        expect(instance).to receive(:keys)
          .and_return([1, 2, 3, 4])

        expect(instance).to receive(:delete)
          .with(1, 2, 3, 4)

        instance.clear
      end
    end

    describe "#delete" do
      subject { instance.delete(*keys) }

      context "no keys" do
        let(:keys) {}

        it { is_expected.to eq(nil) }
      end

      context "no keys" do
        let(:keys) { [1, 2, 3, 4] }

        before do
          allow(redis_client_dbl).to receive(:call)
            .with("DEL", encoded_key(1), encoded_key(2), encoded_key(3), encoded_key(4))
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
              "MSET",
              encoded_key(:a), JSON.generate({a: 1}),
              encoded_key(:b), JSON.generate({b: 2})
            )
            .and_return("foobar")
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
              "MGET", encoded_key("a"), encoded_key("b"), encoded_key("c")
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
              "MGET", encoded_key("x"), encoded_key("y")
            ).and_return([nil, nil])
        end

        it { is_expected.to eq({}) }
      end
    end

    describe "#empty?" do
      subject { instance.empty? }

      before do
        allow(instance).to receive(:keys)
          .and_return(keys)
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
