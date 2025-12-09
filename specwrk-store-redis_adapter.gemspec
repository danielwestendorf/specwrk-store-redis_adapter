# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "specwrk-store-redis_adapter"
  spec.version = "0.1.0"
  spec.authors = ["Daniel Westendorf"]
  spec.email = ["daniel@prowestech.com"]

  spec.summary = "Redis adapater for Specwrk, a parallel RSpec test runner"
  spec.homepage = "https://github.com/danielwestendorf/specwrk-store-redis_adapter"
  spec.license = "GPL-3.0-or-later"
  spec.required_ruby_version = ">= 3.0.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = spec.homepage + "/blob/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end

  spec.require_paths = ["lib"]

  spec.add_dependency "specwrk", "~> 0.19.1"
  spec.add_dependency "redis-client"

  spec.add_development_dependency "standard"
  spec.add_development_dependency "msgpack"
end
