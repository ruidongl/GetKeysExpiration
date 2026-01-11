# GetKeysExpiration

Utility for scanning Azure Cache for Redis instances and reporting keys without expirations (TTL = -1).
Each reported entry includes the hours elapsed since the key was last accessed, and (optionally) a preview of the key's value.

## Prerequisites
- .NET 9.0 SDK or later
- Azure Cache for Redis connection string with administrative access (allows key enumeration)

## Running from Source
```
dotnet run --project GetKeysExpiration/GetKeysExpiration [<connection-string>] [options]
```

#### Options
- `--output <path>` / `-o <path>`: write results to a file (plain text). Files rotate when they grow beyond 2 MB (`output.txt`, `output.part2.txt`, ...). Adjust the threshold by updating the `MaxOutputFileBytes` constant in [GetKeysExpiration/Program.cs](GetKeysExpiration/Program.cs#L8).
- `--value` / `-v`: include a truncated value preview for each key (strings, lists, hashes, sets, sorted sets, streams).
- When `--output` is omitted, results stream to standard output.

### Environment Overrides
- `AZURE_REDIS_CONNECTION_STRING`: used when no connection string argument is provided.
- `AZURE_REDIS_SCAN_PAGE_SIZE`: page size for SCAN operations (default 2048).
- `AZURE_REDIS_KEY_PATTERN`: pattern limiter (default `*`).

### Progress Reporting
If the Redis server supports `DBSIZE`, the tool prints progress updates showing the percentage of keys scanned.

## Example
```
dotnet run --project GetKeysExpiration/GetKeysExpiration "redis-host:6380,password=***,ssl=True" --output missing-ttl.txt --value
```
Replace the placeholder connection string with your own secure credentials.

## PowerShell Script

Requirements:
- `StackExchange.Redis` NuGet package installed for the current user (`Install-Package StackExchange.Redis -Scope CurrentUser`).

Usage:
```
pwsh ./PowerShell/Get-RedisKeysWithoutTtl.ps1 -ConnectionString "redis-host:6380,password=***,ssl=True" [-OutputPath missing-ttl.txt] [-ScanPageSize 2048] [-KeyPattern "*"]
```
- Output can be redirected with `-OutputPath`. When omitted, keys stream to the pipeline.
- Progress percentages appear when the cache supports `DBSIZE`.
