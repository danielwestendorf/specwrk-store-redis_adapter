# frozen_string_literal: true

RSpec.describe "gemspec" do
  subject(:requirement) do
    specification = Gem::Specification.load(File.expand_path("../specwrk-store-redis_adapter.gemspec", __dir__))
    specification.runtime_dependencies.find { |dependency| dependency.name == "specwrk" }.requirement
  end

  it "requires Specwrk 0.19.4 or newer without an upper bound" do
    expect(requirement).not_to be_satisfied_by(Gem::Version.new("0.19.3"))
    expect(requirement).to be_satisfied_by(Gem::Version.new("0.19.4"))
    expect(requirement).to be_satisfied_by(Gem::Version.new("0.20.0"))
  end
end
