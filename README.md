# Redis Store Adapter for Specwrk

A redis storage adapter for persistent [specwrk](https://github.com/danielwestendorf/specwrk) servers, a multi-process/multi-node RSpec test runner. 

## Usage
```
gem install specwrk-store-redis_adapter 
```

Then set the `SPECWRK_SRV_STORE_URI` environment variable to the Redis URL of your redis server.

```
export SPECWRK_SRV_STORE_URI=redis://:supersecret@localhost:6378/0
```
