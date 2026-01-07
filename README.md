# GetKeysExpiration

Utility for scanning Azure Cache for Redis instances and reporting keys without expirations (TTL = -1).

## Prerequisites
- .NET 9.0 SDK or later
- Azure Cache for Redis connection string with administrative access (allows key enumeration)

## Running from Source
```
dotnet run --project GetKeysExpiration/GetKeysExpiration "<connection-string>" [--output <path>]
```
- `--output` (or `-o`) writes the list of keys without TTL to the specified file.
- When `--output` is omitted, key names are printed to standard output.

### Environment Overrides
- `AZURE_REDIS_CONNECTION_STRING`: used when no connection string argument is provided.
- `AZURE_REDIS_SCAN_PAGE_SIZE`: page size for SCAN operations (default 2048).
- `AZURE_REDIS_KEY_PATTERN`: pattern limiter (default `*`).

### Progress Reporting
If the Redis server supports `DBSIZE`, the tool prints progress updates showing the percentage of keys scanned.

## Example
```
dotnet run --project GetKeysExpiration/GetKeysExpiration "redis-host:6380,password=***,ssl=True" --output missing-ttl.txt
```
Replace the placeholder connection string with your own secure credentials.
