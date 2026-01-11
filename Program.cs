using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using StackExchange.Redis;
using System.Linq;

const long MaxOutputFileBytes = 2L * 1024 * 1024; // 2 MB per partition
const int StringValuePreviewLimit = 2048;
const int CollectionPreviewLimit = 16;
const int ElementPreviewLimit = 256;

return await RunAsync(args);

static async Task<int> RunAsync(string[] args)
{
    StreamWriter? outputWriter = null;
    FileStream? outputStream = null;
    long writtenBytes = 0;
    var partitionIndex = 0;
    string? baseOutputPath = null;
    UTF8Encoding? encoding = null;
    var hasOutputFile = false;

    string GetPartitionPath(int index)
    {
        if (string.IsNullOrWhiteSpace(baseOutputPath))
        {
            throw new InvalidOperationException("Output path not specified.");
        }

        if (index == 0)
        {
            return baseOutputPath;
        }

        var directory = Path.GetDirectoryName(baseOutputPath);
        var name = Path.GetFileNameWithoutExtension(baseOutputPath);
        var extension = Path.GetExtension(baseOutputPath);
        var partName = $"{name}.part{index + 1}{extension}";
        return string.IsNullOrEmpty(directory) ? partName : Path.Combine(directory, partName);
    }

    void OpenOutputWriter(int index)
    {
        if (encoding is null)
        {
            throw new InvalidOperationException("Encoding not initialised.");
        }

        var path = GetPartitionPath(index);
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        outputStream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        outputWriter = new StreamWriter(outputStream, encoding);
        writtenBytes = 0;
    }

    void RotateOutput()
    {
        outputWriter?.Flush();
        outputWriter?.Dispose();
        outputStream?.Dispose();
        partitionIndex++;
        OpenOutputWriter(partitionIndex);
    }

    void WriteToOutput(string message)
    {
        if (!hasOutputFile)
        {
            return;
        }

        if (encoding is null)
        {
            throw new InvalidOperationException("Encoding not initialised.");
        }

        var byteCount = encoding.GetByteCount(message) + encoding.GetByteCount(Environment.NewLine);

        if (outputWriter is null)
        {
            OpenOutputWriter(0);
        }
        else if (writtenBytes > 0 && writtenBytes + byteCount > MaxOutputFileBytes)
        {
            RotateOutput();
        }

        outputWriter!.WriteLine(message);
        writtenBytes += byteCount;
    }

    void FlushOutput() => outputWriter?.Flush();

    void DisposeOutput()
    {
        outputWriter?.Flush();
        outputWriter?.Dispose();
        outputStream?.Dispose();
    }

    try
    {
        var (argConnectionString, outputPath, includeValues) = ParseArguments(args);
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

        encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        baseOutputPath = outputPath;
        hasOutputFile = !string.IsNullOrWhiteSpace(baseOutputPath);
        outputWriter = null;
        outputStream = null;
        writtenBytes = 0;
        partitionIndex = 0;

        void WriteKeyLine(string message)
        {
            if (!hasOutputFile)
            {
                Console.WriteLine(message);
                return;
            }

            WriteToOutput(message);
        }

        void WriteSummary(string message)
        {
            Console.WriteLine(message);
            WriteToOutput(message);
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
                    missingExpiryCount += await ProcessBatchAsync(db, buffer, count, includeValues, WriteKeyLine);
                    ReportProgress(count);
                    count = 0;
                }
            }

            if (count > 0)
            {
                missingExpiryCount += await ProcessBatchAsync(db, buffer, count, includeValues, WriteKeyLine);
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

        FlushOutput();

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
    finally
    {
        DisposeOutput();
    }
}

static async Task<int> ProcessBatchAsync(IDatabase db, RedisKey[] buffer, int count, bool includeValues, Action<string> writeKeyLine)
{
    var batch = db.CreateBatch();
    var ttlTasks = new Task<TimeSpan?>[count];
    var idleTasks = new Task<TimeSpan?>[count];
    Task<RedisType>[]? typeTasks = includeValues ? new Task<RedisType>[count] : null;

    for (var i = 0; i < count; i++)
    {
        ttlTasks[i] = batch.KeyTimeToLiveAsync(buffer[i]);
        idleTasks[i] = batch.KeyIdleTimeAsync(buffer[i]);

        if (typeTasks is not null)
        {
            typeTasks[i] = batch.KeyTypeAsync(buffer[i]);
        }
    }

    batch.Execute();
    await Task.WhenAll(ttlTasks);
    await Task.WhenAll(idleTasks);

    if (typeTasks is not null)
    {
        await Task.WhenAll(typeTasks);
    }

    var missing = 0;

    for (var i = 0; i < count; i++)
    {
        var ttl = ttlTasks[i].Result;

        if (!ttl.HasValue)
        {
            var idle = idleTasks[i].Result;
            var idleHoursText = idle.HasValue
                ? idle.Value.TotalHours.ToString("F2", CultureInfo.InvariantCulture)
                : "unknown";

            string? valueText = null;

            if (includeValues)
            {
                var type = typeTasks![i].Result;
                valueText = await GetValueRepresentationAsync(db, buffer[i], type).ConfigureAwait(false);
            }

            var lineBuilder = new StringBuilder();
            lineBuilder.Append(buffer[i]);
            lineBuilder.Append(" | TTL: -1 | IdleHours: ");
            lineBuilder.Append(idleHoursText);

            if (includeValues)
            {
                lineBuilder.Append(" | Value: ");
                lineBuilder.Append(valueText);
            }

            writeKeyLine(lineBuilder.ToString());
            missing++;
        }
    }

    return missing;
}

static (string? connectionString, string? outputPath, bool includeValues) ParseArguments(string[] args)
{
    string? connectionString = null;
    string? outputPath = null;
    var includeValues = false;

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

        if (string.Equals(arg, "--value", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(arg, "-v", StringComparison.OrdinalIgnoreCase))
        {
            includeValues = true;
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

    return (connectionString, outputPath, includeValues);
}

static async Task<string> GetValueRepresentationAsync(IDatabase db, RedisKey key, RedisType keyType)
{
    switch (keyType)
    {
        case RedisType.None:
            return "(key missing)";
        case RedisType.String:
            var stringValue = await db.StringGetAsync(key).ConfigureAwait(false);
            if (!stringValue.HasValue)
            {
                return "(null)";
            }

            return NormalizeAndTruncate(stringValue.ToString(), StringValuePreviewLimit);
        case RedisType.List:
            return await FormatListAsync(db, key).ConfigureAwait(false);
        case RedisType.Hash:
            return await FormatHashAsync(db, key).ConfigureAwait(false);
        case RedisType.Set:
            return await FormatSetAsync(db, key).ConfigureAwait(false);
        case RedisType.SortedSet:
            return await FormatSortedSetAsync(db, key).ConfigureAwait(false);
        case RedisType.Stream:
            return await FormatStreamAsync(db, key).ConfigureAwait(false);
        default:
            return $"<{keyType} value not retrieved>";
    }
}

static async Task<string> FormatListAsync(IDatabase db, RedisKey key)
{
    var rangeTask = db.ListRangeAsync(key, 0, CollectionPreviewLimit - 1);
    var lengthTask = db.ListLengthAsync(key);
    await Task.WhenAll(rangeTask, lengthTask).ConfigureAwait(false);

    var preview = rangeTask.Result.Select(FormatElement).ToList();
    return FormatSequence("List", lengthTask.Result, preview);
}

static async Task<string> FormatHashAsync(IDatabase db, RedisKey key)
{
    var totalCount = await db.HashLengthAsync(key).ConfigureAwait(false);
    var preview = db.HashScan(key)
        .Take(CollectionPreviewLimit)
        .Select(entry => ($"{FormatElement(entry.Name)}", FormatElement(entry.Value)))
        .ToList();

    return FormatKeyValueSequence("Hash", totalCount, preview);
}

static async Task<string> FormatSetAsync(IDatabase db, RedisKey key)
{
    var totalCount = await db.SetLengthAsync(key).ConfigureAwait(false);
    var preview = db.SetScan(key)
        .Take(CollectionPreviewLimit)
        .Select(FormatElement)
        .ToList();

    return FormatSequence("Set", totalCount, preview);
}

static async Task<string> FormatSortedSetAsync(IDatabase db, RedisKey key)
{
    var totalCount = await db.SortedSetLengthAsync(key).ConfigureAwait(false);
    var preview = db.SortedSetScan(key)
        .Take(CollectionPreviewLimit)
        .Select(entry => $"{FormatElement(entry.Element)}@{entry.Score.ToString("G", CultureInfo.InvariantCulture)}")
        .ToList();

    return FormatSequence("SortedSet", totalCount, preview);
}

static async Task<string> FormatStreamAsync(IDatabase db, RedisKey key)
{
    var entries = await db.StreamRangeAsync(key, count: CollectionPreviewLimit).ConfigureAwait(false);
    var totalCount = await db.StreamLengthAsync(key).ConfigureAwait(false);

    var preview = entries
        .Select(entry =>
        {
            var fieldPreview = entry.Values
                .Take(CollectionPreviewLimit)
                .Select(kv => $"{FormatElement(kv.Name)}={FormatElement(kv.Value)}")
                .ToList();

            var suffix = entry.Values.Length > fieldPreview.Count ? ", ..." : string.Empty;
            return $"{FormatElement(entry.Id)} {{{string.Join(", ", fieldPreview)}{suffix}}}";
        })
        .ToList();

    return FormatSequence("Stream", totalCount, preview);
}

static string FormatSequence(string typeName, long totalCount, IList<string> preview)
{
    var suffix = totalCount > preview.Count ? ", ..." : string.Empty;
    var joined = preview.Count > 0 ? string.Join(", ", preview) : string.Empty;
    return $"{typeName}[{totalCount}] [{joined}{suffix}]";
}

static string FormatKeyValueSequence(string typeName, long totalCount, IList<(string Name, string Value)> preview)
{
    var suffix = totalCount > preview.Count ? ", ..." : string.Empty;
    var joined = preview.Count > 0
        ? string.Join(", ", preview.Select(p => $"{p.Name}={p.Value}"))
        : string.Empty;
    return $"{typeName}[{totalCount}] {{{joined}{suffix}}}";
}

static string FormatElement(RedisValue value)
{
    if (value.IsNull)
    {
        return "(null)";
    }

    return NormalizeAndTruncate(value.ToString(), ElementPreviewLimit);
}

static string NormalizeAndTruncate(string value, int limit)
{
    var normalized = value.Replace("\r", "\\r").Replace("\n", "\\n");
    if (normalized.Length <= limit)
    {
        return normalized;
    }

    return normalized.Substring(0, limit) + "…";
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

