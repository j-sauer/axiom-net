using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace AxiomNet
{
    /// <summary>
    /// Describes the content type of the data to ingest
    /// </summary>
    public enum ContentType
    {
        /// <summary>
        /// Treats the data as JSON array
        /// </summary>
        Json,
        /// <summary>
        /// Treats the data as newline delimited JSON objects. Preferred data format
        /// </summary>
        Ndjson,
        /// <summary>
        /// Treats the data as CSV content
        /// </summary>
        Csv
    }

    /// <summary>
    /// Describes the content encoding of the data to ingest
    /// </summary>
    public enum ContentEncoding
    {
        /// <summary>
        /// Marks the data as not being encoded
        /// </summary>
        Identity,
        /// <summary>
        /// Marks the data as being gzip encoded. Preferred compression format
        /// </summary>
        Gzip,
        /// <summary>
        /// Marks the data as being zstd encoded
        /// </summary>
        Zstd
    }

    /// <summary>
    /// Dataset represents an Axiom dataset
    /// </summary>
    public record Dataset
    {
        /// <summary>
        /// ID of the dataset
        /// </summary>
        [JsonPropertyName("id")]
        public string Id { get; set; }

        /// <summary>
        /// The unique name of the dataset
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; }

        /// <summary>
        /// Description of the dataset
        /// </summary>
        [JsonPropertyName("description")]
        public string Description { get; set; }

        /// <summary>
        /// The ID of the user who created the dataset
        /// </summary>
        [JsonPropertyName("who")]
        public string CreatedBy { get; set; }

        /// <summary>
        /// The time the dataset was created at
        /// </summary>
        [JsonPropertyName("created")]
        public DateTime CreatedAt { get; set; }
    }

    /// <summary>
    /// Represents a field of an Axiom dataset
    /// </summary>
    public record Field
    {
        /// <summary>
        /// THe unique name of the field
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; }

        /// <summary>
        /// Description of the field
        /// </summary>
        [JsonPropertyName("description")]
        public string Description { get; set; }

        /// <summary>
        /// The datatype of the field
        /// </summary>
        [JsonPropertyName("type")]
        public string Type { get; set; }

        /// <summary>
        /// The unit of the field
        /// </summary>
        [JsonPropertyName("unit")]
        public string Unit { get; set; }

        /// <summary>
        /// Describes of the field is hidden or not
        /// </summary>
        [JsonPropertyName("hidden")]
        public bool Hidden { get; set; }
    }

    /// <summary>
    /// Represents the details of the information stored inside a dataset including the fields that make up the dataset
    /// </summary>
    public record DatasetInfo
    {
        /// <summary>
        /// The unique name of the dataset
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; }

        /// <summary>
        /// The number of blocks of the dataset.
        /// </summary>
        [JsonPropertyName("numBlocks")]
        public long NumBlocks { get; set; }

        /// <summary>
        /// The number of events of the dataset
        /// </summary>
        [JsonPropertyName("numEvents")]
        public long NumEvents { get; set; }

        /// <summary>
        /// The number of fields of the dataset
        /// </summary>
        [JsonPropertyName("numFields")]
        public int NumFields { get; set; }

        /// <summary>
        /// The amount of data stored in the dataset
        /// </summary>
        [JsonPropertyName("inputBytes")]
        public long InputBytes { get; set; }

        /// <summary>
        /// The amount of data stored in the dataset formatted in a human readable format
        /// </summary>
        [JsonPropertyName("inputBytesHuman")]
        public string InputBytesHuman { get; set; }

        /// <summary>
        /// The amount of compressed data stored in the dataset
        /// </summary>
        [JsonPropertyName("compressedBytes")]
        public long CompressedBytes { get; set; }

        /// <summary>
        /// The amount of compressed data stored in the dataset formatted in a human readable format
        /// </summary>
        [JsonPropertyName("compressedBytesHuman")]
        public string CompressedBytesHuman { get; set; }

        /// <summary>
        /// The time of the oldest event stored in the dataset
        /// </summary>
        [JsonPropertyName("minTime")]
        public DateTime? MinTime { get; set; }

        /// <summary>
        /// The time of the newest event stored in the dataset
        /// </summary>
        [JsonPropertyName("maxTime")]
        public DateTime? MaxTime { get; set; }

        /// <summary>
        /// The fields of the dataset
        /// </summary>
        [JsonPropertyName("fields")]
        public Field[] Fields { get; set; }

        /// <summary>
        /// The ID of the user who created the dataset
        /// </summary>
        [JsonPropertyName("who")]
        public string CreatedBy { get; set; }

        /// <summary>
        /// The time the dataset was created
        /// </summary>
        [JsonPropertyName("created")]
        public DateTime CreatedAt { get; set; }
    }

    /// <summary>
    /// The statistics of all datasets as well as their aggregated totals
    /// </summary>
    public record DatasetStats
    {
        /// <summary>
        /// The individual statistics of all datasets
        /// </summary>
        [JsonPropertyName("datasets")]
        public DatasetInfo[] Datasets { get; set; }

        /// <summary>
        /// The total number of blocks
        /// </summary>
        [JsonPropertyName("numBlocks")]
        public long NumBlocks { get; set; }

        /// <summary>
        /// The total number of events
        /// </summary>
        [JsonPropertyName("numEvents")]
        public long NumEvents { get; set; }

        /// <summary>
        /// The total amount of data stored
        /// </summary>
        [JsonPropertyName("inputBytes")]
        public long InputBytes { get; set; }

        /// <summary>
        /// The total amount of data stored formatted in a human readable format
        /// </summary>
        [JsonPropertyName("inputBytesHuman")]
        public string InputBytesHuman { get; set; }

        /// <summary>
        /// THe total amount of compressed data stored
        /// </summary>
        [JsonPropertyName("compressedBytes")]
        public long CompressedBytes { get; set; }

        /// <summary>
        /// The total amount of compressed data stored formatted in a human readable format
        /// </summary>
        [JsonPropertyName("compressedBytesHuman")]
        public string CompressedBytesHuman { get; set; }
    }

    /// <summary>
    /// A request used to create a dataset
    /// </summary>
    public record DatasetCreateRequest
    {
        /// <summary>
        /// Name of the dataset to create. Restricted to 80 characters of [a-zA-Z0-9]
        /// and special characters "-", "_" and ".". Special characters cannot be a
        /// prefix or suffix. The prefix cannot be "axiom-".
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; }

        /// <summary>
        /// Description of the dataset to create
        /// </summary>
        [JsonPropertyName("description")]
        public string Description { get; set; }
    }

    /// <summary>
    /// A request used to update a dataset
    /// </summary>
    public record DatasetUpdateRequest
    {
        /// <summary>
        /// Description of the dataset to update
        /// </summary>
        [JsonPropertyName("description")]
        public string Description { get; set; }
    }

    /// <summary>
    /// A request used to update a field for a dataset
    /// </summary>
    public record FieldUpdateRequest
    {
        /// <summary>
        /// Description of the field to update
        /// </summary>
        [JsonPropertyName("description")]
        public string Description { get; set; }

        /// <summary>
        /// Unit of the field to update
        /// </summary>
        [JsonPropertyName("unit")]
        public string Unit { get; set; }

        /// <summary>
        /// Hidden status of the field to update
        /// </summary>
        [JsonPropertyName("hidden")]
        public bool Hidden { get; set; }
    }

    /// <summary>
    /// Describes the ingestion failure of a single event
    /// </summary>
    public record IngestFailure
    {
        /// <summary>
        /// Timestamp of the event that failed to ingest
        /// </summary>
        [JsonPropertyName("timestamp")]
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// Error that made the event fail to ingest
        /// </summary>
        [JsonPropertyName("error")]
        public string Error { get; set; }
    }

    /// <summary>
    /// The status after an event ingestion operation
    /// </summary>
    public record IngestStatus
    {
        /// <summary>
        /// The status after an event ingestion operation
        /// </summary>
        [JsonPropertyName("ingested")]
        public ulong Ingested { get; set; }

        /// <summary>
        /// The amount of events that failed to ingest
        /// </summary>
        [JsonPropertyName("failed")]
        public ulong Failed { get; set; }

        /// <summary>
        /// The ingestion failures, if any
        /// </summary>
        [JsonPropertyName("failures")]
        public IngestFailure[] Failures { get; set; }

        /// <summary>
        /// The number of bytes processed
        /// </summary>
        [JsonPropertyName("processedBytes")]
        public ulong ProcessedBytes { get; set; }

        /// <summary>
        /// The amount of blocks created
        /// </summary>
        [JsonPropertyName("blocksCreated")]
        public uint BlocksCreated { get; set; }

        /// <summary>
        /// The length of the Write-Ahead Log
        /// </summary>
        [JsonPropertyName("walLength")]
        public uint WalLength { get; set; }
    }

    internal record DatasetTrimRequest
    {
        /// <summary>
        /// MaxDuration marks the oldest timestamp an event can have before getting deleted
        /// </summary>
        [JsonPropertyName("maxDuration")]
        public string MaxDuration { get; set; }
    }

    /// <summary>
    /// The result of a trim operation
    /// </summary>
    public record DatasetTrimResult
    {
        /// <summary>
        /// The amount of blocks deleted by the trim operation
        /// </summary>
        [JsonPropertyName("numDeleted")]
        public int BlocksDeleted { get; set; }
    }

    /// <summary>
    /// Specifies the optional parameters for the Ingest and IngestEvents method
    /// </summary>
    public record IngestOptions
    {
        /// <summary>
        /// Defines a custom field to extract the ingestion timestamp from. Defaults to `_time`
        /// </summary>
        public string? TimestampField { get; set; }

        /// <summary>
        /// Defines a custom format for the TimestampField.
        /// The reference time is `Mon Jan 2 15:04:05 -0700 MST 2006`, as specified
        /// in https://pkg.go.dev/time/?tab=doc#Parse
        /// </summary>
        public string? TimestampFormat { get; set; }

        /// <summary>
        /// The delimiter that separates CSV fields. Only valid when the content to be ingested is CSV formatted
        /// </summary>
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

        /// <summary>
        /// Stats returns detailed statistics about all available datasets.
        ///
        /// This operation is expensive and listing the datasets and then retrieving
        /// the information of a specific dataset is preferred, when no aggregated
        /// statistics across all datasets are needed.
        /// </summary>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>The statistics of all datasets</returns>
        public async Task<DatasetStats> Stats(CancellationToken ct)
        {
            DatasetStats res = await _client.Call<DatasetStats>(HttpMethod.Get, BasePath + "/_stats", ct);

            return res;
        }

        /// <summary>
        /// Gets a dataset by id.
        /// </summary>
        /// <param name="id">The id of the dataset.</param>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>The dataset with the given id.</returns>
        public async Task<Dataset> Get(string id, CancellationToken ct)
        {
            Dataset res = await _client.Call<Dataset>(HttpMethod.Get, BasePath + "/" + id, ct);

            return res;
        }

        /// <summary>
        /// Lists all available datasets.
        /// </summary>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>List of all available datasets.</returns>
        public async Task<IReadOnlyList<Dataset>> List(CancellationToken ct)
        {
            Dataset[] res = await _client.Call<Dataset[]>(HttpMethod.Get, BasePath, ct);

            return res;
        }

        /// <summary>
        /// Creates a dataset with a unique name and a description.
        /// </summary>
        /// <param name="name">The unique name of the dataset.</param>
        /// <param name="description">The description for the dataset.</param>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>The created dataset.</returns>
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

        /// <summary>
        /// Updates with dataset with the given id.
        /// </summary>
        /// <param name="id">The id of the dataset.</param>
        /// <param name="description">The new description of the dataset.</param>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>The updated dataset.</returns>
        public async Task<Dataset> Update(string id, string description, CancellationToken ct)
        {
            DatasetUpdateRequest req = new DatasetUpdateRequest()
            {
                Description = description
            };

            Dataset res = await _client.Call<Dataset, DatasetUpdateRequest>(HttpMethod.Put, BasePath + "/" + id, req, ct);

            return res;
        }

        /// <summary>
        /// Updates the named field of the dataset identified by the given id with the given properties.
        /// </summary>
        /// <param name="id">The id of the dataset.</param>
        /// <param name="field">The field name.</param>
        /// <param name="req">The field properties.</param>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>The updated field.</returns>
        public async Task<Field> UpdateField(string id, string field, FieldUpdateRequest req, CancellationToken ct)
        {
            string path = BasePath + "/" + id + "/fields/" + field;
            Field res = await _client.Call<Field, FieldUpdateRequest>(HttpMethod.Put, path, req, ct);

            return res;
        }

        /// <summary>
        /// Deletes a dataset.
        /// </summary>
        /// <param name="id">The id of the dataset.</param>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        public async Task Delete(string id, CancellationToken ct)
        {
            await _client.Call(HttpMethod.Delete, BasePath + "/" + id, ct);
        }

        /// <summary>
        /// Retrieves the information of the dataset identified by its id.
        /// </summary>
        /// <param name="id">THe id of the dataset.</param>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>Information of the dataset.</returns>
        public async Task<DatasetInfo> Info(string id, CancellationToken ct)
        {
            string path = BasePath + "/" + id + "/info";
            DatasetInfo res = await _client.Call<DatasetInfo>(HttpMethod.Get, path, ct);
            return res;
        }

        /// <summary>
        /// Ingests data into the dataset identified by its id. Restrictions for field
        /// names (JSON object keys) can be reviewed here:
        /// https://www.axiom.co/docs/usage/field-restrictions
        /// </summary>
        /// <param name="id">The id of the dataset.</param>
        /// <param name="s">A stream pointing to the events.</param>
        /// <param name="contentType">The content type of the events.</param>
        /// <param name="contentEncoding">The content encoding of the events.</param>
        /// <param name="options">Ingest options</param>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>The result of the ingest operation.</returns>
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

        /// <summary>
        /// Ingests events into the dataset identified by its id.
        /// Restrictions for field names (JSON object keys) can be reviewed here:
        /// https://www.axiom.co/docs/usage/field-restrictions
        /// This method uses NDJSON and GZIP for sending the events.
        /// </summary>
        /// <param name="id">The id of the dataset.</param>
        /// <param name="events">A list of events.</param>
        /// <param name="options">Ingest options</param>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>The result of the ingest operation.</returns>
        public async Task<IngestStatus> IngestEvents(string id, IReadOnlyList<object> events,
            IngestOptions? options, CancellationToken ct)
        {
            string[] serializedEvents = new string[events.Count];
            for (int i = 0; i < events.Count; i++)
            {
                serializedEvents[i] = JsonSerializer.Serialize(events[i]);
            }

            string contentAsString = string.Join("\n", serializedEvents) + "\n";

            byte[] content = Encoding.UTF8.GetBytes(contentAsString);

            using MemoryStream ms = new MemoryStream();
            using (GZipStream gZipStream = new GZipStream(ms, CompressionMode.Compress))
            {
                gZipStream.Write(content, 0, content.Length);
            }

            using MemoryStream s = new MemoryStream(ms.ToArray());

            return await Ingest(id, s, ContentType.Ndjson, ContentEncoding.Gzip, options, ct);
        }

        /// <summary>
        /// Trims the dataset identified by its id to a given length. The max duration
        /// given will mark the oldest timestamp an event can have. Older ones will be
        /// deleted from the dataset.
        /// </summary>
        /// <param name="id">The id of the dataset.</param>
        /// <param name="duration">The max duration
        /// that marks the oldest timestamp an event can have.</param>
        /// <param name="ct">A cancellation token to cancel the request.</param>
        /// <returns>The result of the trim operation.</returns>
        public async Task<DatasetTrimResult> Trim(string id, TimeSpan duration, CancellationToken ct)
        {
            string path = BasePath + "/" + id + "/trim";

            string durationS = $"{duration.Seconds}s";
            if (duration.Minutes > 0)
                durationS = $"{duration.Minutes}m{durationS}";
            int hours = duration.Days * 24 + duration.Hours;
            if (hours > 0)
                durationS = $"{hours}h{durationS}";

            DatasetTrimRequest req = new DatasetTrimRequest()
            {
                MaxDuration = durationS
            };

            DatasetTrimResult res = await _client.Call<DatasetTrimResult, DatasetTrimRequest>(HttpMethod.Post, path, req, ct);
            return res;
        }
    }
}