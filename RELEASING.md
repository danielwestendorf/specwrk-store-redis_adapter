# Releasing the Specwrk Redis Store Adapter

Releases are cut from `main` in two stages:

1. `gem bump` updates the version for the release.
2. Bundler's `rake release` task tags the commit, pushes Git, and publishes the gem.

## One-time setup

Install `gem-release`, which provides the `gem bump` command. It is intentionally
not a development dependency because it is only needed by maintainers cutting a
release.

```sh
gem install gem-release
```

Configure credentials for RubyGems:

```sh
gem signin
```

Pushing this gem to RubyGems requires multi-factor authentication. Run the
release from an interactive terminal and have your authenticator available.
RubyGems will prompt for an OTP code or WebAuthn verification when `rake release`
pushes the gem. Signing in stores an API key, but does not bypass the MFA check
required for each push.

## Cut a release

Start from an up-to-date, clean `main` branch. This is important because
`rake release` pushes the current branch.

```sh
git switch main
git pull --ff-only origin main
git status --short
```

Bump the version without committing. Use `patch`, `minor`, `major`, or an
explicit version number:

```sh
gem bump --version patch --no-commit
```

This updates `lib/specwrk/store/redis_adapter/version.rb` while leaving the
release changes uncommitted. Update `CHANGELOG.md` using the new version:

1. Add a dated section for the new version.
2. Move the Unreleased entries into that section.
3. Leave an empty Unreleased section for subsequent changes.

Stage the version, changelog, and any release documentation, then create the
version commit:

```sh
git add lib/specwrk/store/redis_adapter/version.rb CHANGELOG.md RELEASING.md
git commit -m "Bump specwrk-store-redis_adapter to VERSION"
```

Replace `VERSION` with the new version, for example `0.1.1`. Confirm that the
commit contains the version and changelog updates:

```sh
git show --stat --oneline HEAD
git status --short
```

Run the full checks:

```sh
bundle exec rake
```

Publish the release:

```sh
bundle exec rake release
```

The release task requires a clean tracked worktree and then:

1. Builds `pkg/specwrk-store-redis_adapter-VERSION.gem`.
2. Creates the annotated `vVERSION` tag.
3. Pushes `main` and the tag to `origin`.
4. Pushes the gem to RubyGems.org and prompts for MFA verification.

Use `gem bump` only to update the version; use `bundle exec rake release`, not
`gem release`, to publish this project.

If the RubyGems push fails after the tag was pushed, fix the authentication or
network problem and rerun `bundle exec rake release`. The task recognizes the
existing tag and retries the gem publication.
