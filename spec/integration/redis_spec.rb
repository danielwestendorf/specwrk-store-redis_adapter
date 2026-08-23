# frozen_string_literal: true

RSpec.describe "Redis Adapter" do
  let(:uri) { "redis://localhost:6379/8" }

  it "locks" do
    foo = 1

    Specwrk::Store::RedisAdapter.with_lock(uri, "foobar") do
      foo += 1
    end

    expect(foo).to eq(2)
  end

  it "raises when the lock is unavailable" do
    lock_acquired = Queue.new
    release_lock = Queue.new

    thread = Thread.new do
      Specwrk::Store::RedisAdapter.with_lock(uri, "foobar") do
        lock_acquired.push(true)
        release_lock.pop
      end
    end

    begin
      lock_acquired.pop

      expect do
        Specwrk::Store::RedisAdapter.with_lock(uri, "foobar") { true }
      end.to raise_error(Specwrk::Store::LockUnavailableError)
    ensure
      release_lock.push(true)
      thread.join
    end
  end

  it "does not release a lock owned by another caller" do
    lock_key = "specwrk-lock-ownership-#{SecureRandom.uuid}"

    Specwrk::Store::RedisAdapter.connection_pool_for(uri).with do |connection|
      connection.call("SET", lock_key, "owner", "PX", 10_000)
      connection.call("SCRIPT", "LOAD", Specwrk::Store::RedisAdapter::UNLOCK_SCRIPT)

      result = connection.call(
        "EVALSHA",
        Specwrk::Store::RedisAdapter::UNLOCK_SCRIPT_SHA,
        1,
        lock_key,
        "not-the-owner"
      )

      expect(result).to eq(0)
      expect(connection.call("GET", lock_key)).to eq("owner")
    ensure
      connection.call("DEL", lock_key)
    end
  end

  it "instance methods" do
    instance = Specwrk::Store::RedisAdapter.new(uri, "foobar")
    instance.clear

    expect(instance.empty?).to eq(true)

    instance["foobar"] = {a: 1}
    expect(instance["foobar"]).to eq(a: 1)

    instance["baz"] = true

    expect(instance.keys).to match_array(["foobar", "baz"])
    expect(instance.empty?).to eq(false)

    instance.delete("baz")
    expect(instance.keys).to eq(["foobar"])

    instance.merge!(foobar: 1, baz: 2, blah: 4)
    expect(instance["foobar"]).to eq(1)
    expect(instance["baz"]).to eq(2)

    expect(instance.multi_read("foobar", :baz, "blah", "fake")).to eq("foobar" => 1, :baz => 2, "blah" => 4)

    instance.clear
    expect(instance.empty?).to eq(true)
  end
end
