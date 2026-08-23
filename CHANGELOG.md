## [Unreleased]

## [0.2.0] - 2026-08-23

- Replace the Redis queue lock with atomic, token-owned Lua lock and unlock scripts cached and versioned by content SHA.
- Raise `Specwrk::Store::LockUnavailableError` immediately when a Redis lock cannot be acquired.
- Document the release process and manage versions with `gem-release`.

## [0.1.0] - 2025-08-24

- Initial release
