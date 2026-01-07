using System.Globalization;
using System.Net;
using System.Text;
using StackExchange.Redis;

return await RunAsync(args);

static async Task<int> RunAsync(string[] args)
{
    try
    {
        var (argConnectionString, outputPath) = ParseArguments(args);
        var connectionString = argConnectionString ??
            Environment.GetEnvironmentVariable("AZURE_REDIS_CONNECTION_STRING");

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.Error.WriteLine("Provide the Redis connection string as arg[0] or set AZURE_REDIS_CONNECTION_STRING.");
            return 1;
        }

        var options = ConfigurationOptions.Parse(connectionString);
        options.AllowAdmin = true; // Required to enumerate keys and check TTL in Azure Cache for Redis.

        using var muxer = await ConnectionMultiplexer.ConnectAsync(options);
        var db = muxer.GetDatabase();
        var endpoints = muxer.GetEndPoints();
        var scanPageSize = ParsePositiveInt(Environment.GetEnvironmentVariable("AZURE_REDIS_SCAN_PAGE_SIZE"), 2048);
        var keyPattern = Environment.GetEnvironmentVariable("AZURE_REDIS_KEY_PATTERN") ?? "*";
        await using var fileWriter = !string.IsNullOrWhiteSpace(outputPath)
            ? new StreamWriter(outputPath!, false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false))
            : null;

        void WriteKeyLine(string message)
        {
            if (fileWriter is null)
            {
                Console.WriteLine(message);
            }
            else
            {
                fileWriter.WriteLine(message);
            }
        }

        void WriteSummary(string message)
        {
            Console.WriteLine(message);
            fileWriter?.WriteLine(message);
        }

        if (endpoints.Length == 0)
        {
            Console.Error.WriteLine("No endpoints discovered for the Redis cache.");
            return 1;
        }

        WriteSummary("Keys without expiration:");
        var missingExpiryCount = 0;
        var totalKeyCount = await TryGetTotalKeyCountAsync(muxer, endpoints);
        var processedKeyCount = 0L;
        var lastReportedPercent = -1;

        if (totalKeyCount is null)
        {
            Console.WriteLine("Total key count unavailable; percentage progress disabled.");
        }
        else
        {
            Console.WriteLine($"Total keys reported: {totalKeyCount.Value:N0}");
        }

        void ReportProgress(int batchCount)
        {
            if (totalKeyCount is null || totalKeyCount.Value <= 0)
            {
                return;
            }

            processedKeyCount += batchCount;
            var percent = (int)Math.Floor((double)processedKeyCount * 100 / totalKeyCount.Value);

            if (percent > lastReportedPercent)
            {
                lastReportedPercent = percent;
                Console.WriteLine($"Progress: {processedKeyCount:N0}/{totalKeyCount.Value:N0} ({percent}%) keys scanned.");
            }
        }

        foreach (var endpoint in endpoints)
        {
            var server = muxer.GetServer(endpoint);
            if (!server.IsConnected || server.ServerType == ServerType.Sentinel)
            {
                continue;
            }

            var buffer = new RedisKey[Math.Max(1, scanPageSize)];
            var count = 0;

            await foreach (var key in server.KeysAsync(pattern: keyPattern, pageSize: scanPageSize))
            {
                buffer[count++] = key;

                if (count == buffer.Length)
                {
                    missingExpiryCount += await ProcessBatchAsync(db, buffer, count, WriteKeyLine);
                    ReportProgress(count);
                    count = 0;
                }
            }

            if (count > 0)
            {
                missingExpiryCount += await ProcessBatchAsync(db, buffer, count, WriteKeyLine);
                ReportProgress(count);
            }
        }

        ReportProgress(0);

        if (missingExpiryCount == 0)
        {
            WriteSummary("(none)");
        }
        else
        {
            WriteSummary($"Total keys without expiration: {missingExpiryCount}");
        }

        if (fileWriter is not null)
        {
            await fileWriter.FlushAsync();
        }

        return 0;
    }
    catch (RedisConnectionException ex)
    {
        Console.Error.WriteLine($"Failed to connect to Redis: {ex.Message}");
        return 1;
    }
    catch (RedisServerException ex)
    {
        Console.Error.WriteLine($"Redis server error: {ex.Message}");
        return 1;
    }
    catch (ArgumentException ex)
    {
        Console.Error.WriteLine(ex.Message);
        return 1;
    }
    catch (IOException ex)
    {
        Console.Error.WriteLine($"Failed to write output: {ex.Message}");
        return 1;
    }
}

static async Task<int> ProcessBatchAsync(IDatabase db, RedisKey[] buffer, int count, Action<string> writeKeyLine)
{
    var batch = db.CreateBatch();
    var ttlTasks = new Task<TimeSpan?>[count];

    for (var i = 0; i < count; i++)
    {
        ttlTasks[i] = batch.KeyTimeToLiveAsync(buffer[i]);
    }

    batch.Execute();
    await Task.WhenAll(ttlTasks);

    var missing = 0;

    for (var i = 0; i < count; i++)
    {
        var ttl = ttlTasks[i].Result;

        if (!ttl.HasValue)
        {
            writeKeyLine($"{buffer[i]} | TTL: -1");
            missing++;
        }
    }

    return missing;
}

static (string? connectionString, string? outputPath) ParseArguments(string[] args)
{
    string? connectionString = null;
    string? outputPath = null;

    for (var i = 0; i < args.Length; i++)
    {
        var arg = args[i];

        if (arg.StartsWith("--output=", StringComparison.OrdinalIgnoreCase))
        {
            outputPath = arg.Substring("--output=".Length);
            continue;
        }

        if (string.Equals(arg, "--output", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(arg, "-o", StringComparison.OrdinalIgnoreCase))
        {
            if (i + 1 >= args.Length)
            {
                throw new ArgumentException("Missing value for --output option.");
            }

            outputPath = args[++i];
            continue;
        }

        if (arg.StartsWith("-"))
        {
            throw new ArgumentException($"Unrecognized option: {arg}");
        }

        if (connectionString is not null)
        {
            throw new ArgumentException($"Unexpected argument: {arg}");
        }

        connectionString = arg;
    }

    return (connectionString, outputPath);
}

static async Task<long?> TryGetTotalKeyCountAsync(IConnectionMultiplexer muxer, EndPoint[] endpoints)
{
    long total = 0;
    var countableFound = false;

    foreach (var endpoint in endpoints)
    {
        var server = muxer.GetServer(endpoint);
        if (!server.IsConnected || server.ServerType == ServerType.Sentinel)
        {
            continue;
        }

        try
        {
            var size = await server.DatabaseSizeAsync().ConfigureAwait(false);
            total += size;
            countableFound = true;
        }
        catch (RedisServerException)
        {
            return null;
        }
        catch (NotSupportedException)
        {
            return null;
        }
    }

    return countableFound ? total : 0L;
}

static int ParsePositiveInt(string? value, int defaultValue)
{
    if (int.TryParse(value, out var parsed) && parsed > 0)
    {
        return parsed;
    }

    return Math.Max(1, defaultValue);
}

