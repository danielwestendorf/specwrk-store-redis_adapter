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

  it "sleeps until the lock becomes available" do
    barrier = Queue.new

    thread = Thread.new do
      Specwrk::Store::RedisAdapter.with_lock(uri, "foobar") do
        barrier.push(1)
        sleep 0.25
        barrier.push(1)
      end
    end

    Thread.pass until barrier.length.positive? # wait for other thread to get lock
    expect(barrier.length).to eq(1)

    foo = 1
    result = Specwrk::Store::RedisAdapter.with_lock(uri, "foobar") do
      foo += 1
    end

    expect(result).to eq(2)

    expect(barrier.length).to eq(2)
    thread.join

    expect(foo).to eq(2)
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
