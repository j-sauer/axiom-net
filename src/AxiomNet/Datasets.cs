using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace AxiomHq.Net
{
    public enum ContentType
    {
        Json,
        Ndjson,
        Csv
    }

    public enum ContentEncoding
    {
        Identity,
        Gzip,
        Zstd
    }

    public record Dataset
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }
        [JsonPropertyName("name")]
        public string Name { get; set; }
        [JsonPropertyName("description")]
        public string Description { get; set; }
        [JsonPropertyName("who")]
        public string CreatedBy { get; set; }
        [JsonPropertyName("created")]
        public DateTime CreatedAt { get; set; }
    }

    public record Field
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
        [JsonPropertyName("description")]
        public string Description { get; set; }
        [JsonPropertyName("type")]
        public string Type { get; set; }
        [JsonPropertyName("unit")]
        public string Unit { get; set; }
        [JsonPropertyName("hidden")]
        public bool Hidden { get; set; }
    }

    public record DatasetInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
        [JsonPropertyName("numBlocks")]
        public long NumBlocks { get; set; }
        [JsonPropertyName("numEvents")]
        public long NumEvents { get; set; }
        [JsonPropertyName("numFields")]
        public int NumFields { get; set; }
        [JsonPropertyName("inputBytes")]
        public long InputBytes { get; set; }
        [JsonPropertyName("inputBytesHuman")]
        public string InputBytesHuman { get; set; }
        [JsonPropertyName("compressedBytes")]
        public long CompressedBytes { get; set; }
        [JsonPropertyName("compressedBytesHuman")]
        public string CompressedBytesHuman { get; set; }
        [JsonPropertyName("minTime")]
        public DateTime? MinTime { get; set; }
        [JsonPropertyName("maxTime")]
        public DateTime? MaxTime { get; set; }
        [JsonPropertyName("fields")]
        public Field[] Fields { get; set; }
        [JsonPropertyName("who")]
        public string CreatedBy { get; set; }
        [JsonPropertyName("created")]
        public DateTime CreatedAt { get; set; }
    }

    public record DatasetStats
    {
        [JsonPropertyName("datasets")]
        public DatasetInfo[] Datasets { get; set; }
        [JsonPropertyName("numBlocks")]
        public long NumBlocks { get; set; }
        [JsonPropertyName("numEvents")]
        public long NumEvents { get; set; }
        [JsonPropertyName("inputBytes")]
        public long InputBytes { get; set; }
        [JsonPropertyName("inputBytesHuman")]
        public string InputBytesHuman { get; set; }
        [JsonPropertyName("compressedBytes")]
        public long CompressedBytes { get; set; }
        [JsonPropertyName("compressedBytesHuman")]
        public string CompressedBytesHuman { get; set; }
    }

    public record DatasetCreateRequest
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
        [JsonPropertyName("description")]
        public string Description { get; set; }
    }

    public record DatasetUpdateRequest
    {
        [JsonPropertyName("description")]
        public string Description { get; set; }
    }

    public record FieldUpdateRequest
    {
        [JsonPropertyName("description")]
        public string Description { get; set; }
        [JsonPropertyName("unit")]
        public string Unit { get; set; }
        [JsonPropertyName("hidden")]
        public bool Hidden { get; set; }
    }

    public record IngestFailure
    {
        [JsonPropertyName("timestamp")]
        public DateTime Timestamp { get; set; }
        [JsonPropertyName("error")]
        public string Error { get; set; }
    }

    public record IngestStatus
    {
        [JsonPropertyName("ingested")]
        public ulong Ingested { get; set; }
        [JsonPropertyName("failed")]
        public ulong Failed { get; set; }
        [JsonPropertyName("failures")]
        public IngestFailure[] Failures { get; set; }
        [JsonPropertyName("processedBytes")]
        public ulong ProcessedBytes { get; set; }
        [JsonPropertyName("blocksCreated")]
        public uint BlocksCreated { get; set; }
        [JsonPropertyName("walLength")]
        public uint WalLength { get; set; }
    }

    public record IngestOptions
    {
        public string? TimestampField { get; set; }
        public string? TimestampFormat { get; set; }
        public string? CsvDelimiter { get; set; }
    }

    public class Datasets
    {
        private readonly Client _client;

        private const string TimeStampField = "_time";

        private const string BasePath = "/api/v1/datasets";

        internal Datasets(Client client)
        {
            _client = client;
        }

        public async Task<DatasetStats> Stats(CancellationToken ct)
        {
            DatasetStats res = await _client.Call<DatasetStats>(HttpMethod.Get, BasePath + "/_stats", ct);

            return res;
        }

        public async Task<Dataset> Get(string id, CancellationToken ct)
        {
            Dataset res = await _client.Call<Dataset>(HttpMethod.Get, BasePath + "/" + id, ct);

            return res;
        }

        public async Task<IReadOnlyList<Dataset>> List(CancellationToken ct)
        {
            Dataset[] res = await _client.Call<Dataset[]>(HttpMethod.Get, BasePath, ct);

            return res;
        }

        public async Task<Dataset> Create(string name, string description, CancellationToken ct)
        {
            DatasetCreateRequest req = new DatasetCreateRequest()
            {
                Name = name,
                Description = description
            };

            Dataset res = await _client.Call<Dataset, DatasetCreateRequest>(HttpMethod.Post, BasePath, req, ct);

            return res;
        }

        public async Task<Dataset> Update(string id, string description, CancellationToken ct)
        {
            DatasetUpdateRequest req = new DatasetUpdateRequest()
            {
                Description = description
            };

            Dataset res = await _client.Call<Dataset, DatasetUpdateRequest>(HttpMethod.Put, BasePath + "/" + id, req, ct);

            return res;
        }

        public async Task<Field> UpdateField(string id, string field, FieldUpdateRequest req, CancellationToken ct)
        {
            string path = BasePath + "/" + id + "/fields/" + field;
            Field res = await _client.Call<Field, FieldUpdateRequest>(HttpMethod.Put, path, req, ct);

            return res;
        }

        public async Task Delete(string id, CancellationToken ct)
        {
            await _client.Call(HttpMethod.Delete, BasePath + "/" + id, ct);
        }

        public async Task<DatasetInfo> Info(string id, CancellationToken ct)
        {
            string path = BasePath + "/" + id + "/info";
            DatasetInfo res = await _client.Call<DatasetInfo>(HttpMethod.Get, path, ct);
            return res;
        }

        public async Task<IngestStatus> Ingest(string id, Stream s, ContentType contentType, ContentEncoding contentEncoding,
            IngestOptions? options, CancellationToken ct)
        {
            List<string> queryParams = new List<string>();
            if (options != null)
            {
                if (options.CsvDelimiter != null)
                    queryParams.Add("csv-delimiter=" + HttpUtility.UrlEncode(options.CsvDelimiter));
                if (options.TimestampField != null)
                    queryParams.Add("timestamp-field=" + HttpUtility.UrlEncode(options.TimestampField));
                if (options.TimestampFormat != null)
                    queryParams.Add("timestamp-format=" + HttpUtility.UrlEncode(options.TimestampFormat));
            }
            string path = BasePath + "/" + id + "/ingest";
            if (queryParams.Count > 0)
                path += "?" + string.Join("&", queryParams);

            HttpRequestMessage req = _client.CreateNewRequest(HttpMethod.Post, path, s);

            switch (contentType)
            {
                case ContentType.Json:
                    req.Content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
                    break;
                case ContentType.Ndjson:
                    req.Content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/x-ndjson");
                    break;
                case ContentType.Csv:
                    req.Content.Headers.ContentType = MediaTypeHeaderValue.Parse("text/csv");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(contentType), contentType, null);
            }

            switch (contentEncoding)
            {
                case ContentEncoding.Identity:
                    break;
                case ContentEncoding.Gzip: // gzip
                    req.Content.Headers.ContentEncoding.Add("gzip");
                    break;
                case ContentEncoding.Zstd: // zstd
                    req.Content.Headers.ContentEncoding.Add("zstd");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(contentEncoding), contentEncoding, null);
            }

            IngestStatus res = await _client.Do<IngestStatus>(req, ct);

            return res;
        }

        public async Task<IngestStatus> IngestEvents(string id, IReadOnlyList<object> events,
            IngestOptions? options, CancellationToken ct)
        {
            // NDJSON
            // Zstd
            throw new NotImplementedException();
        }
    }
}