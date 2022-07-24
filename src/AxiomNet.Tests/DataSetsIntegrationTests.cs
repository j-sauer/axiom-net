using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace AxiomNet.Tests
{
    public class DataSetsFixture : IAsyncLifetime
    {
        public DataSetsFixture()
        {
            Uri baseUrl = new Uri(Environment.GetEnvironmentVariable("AXIOM_URL") ?? "http://localhost:8080/axiom/");
            Client = new Client(new HttpClient(), baseUrl, Environment.GetEnvironmentVariable("AXIOM_TOKEN") ??"xapt-274dc2a2-5db4-4f8c-92a3-92e33bee92a8", "axiom");
        }

        public Client Client { get; }
        public Dataset Dataset { get; set; }

        public async Task InitializeAsync()
        {
            string dsName = "test-axiom-go-datasets";
            string dsDescription = "";

            Dataset = await Client.Datasets.Create(dsName, dsDescription, CancellationToken.None);
        }

        public async Task DisposeAsync()
        {
            await Client.Datasets.Delete(Dataset.Id, CancellationToken.None);
        }
    }

    public class DataSetsIntegrationTests : IClassFixture<DataSetsFixture>
    {
        private readonly DataSetsFixture _fixture;

        public DataSetsIntegrationTests(DataSetsFixture fixture)
        {
            _fixture = fixture;
        }

        private Stream GetStream(string s)
        {
            return new MemoryStream(Encoding.UTF8.GetBytes(s));
        }

        [Fact]
        public async void Test()
        {
            await Assert.ThrowsAsync<AxiomException>(async () =>
            {
                await _fixture.Client.Datasets.Create(_fixture.Dataset.Id, "Another description", CancellationToken.None);
            });

            string newDescription = "This is a soon to be filled test dataset";
            Dataset ds = await _fixture.Client.Datasets.Update(_fixture.Dataset.Id, newDescription, CancellationToken.None);

            Assert.NotNull(ds);

            _fixture.Dataset = ds;

            ds = await _fixture.Client.Datasets.Get(_fixture.Dataset.Id, CancellationToken.None);

            Assert.Equal(_fixture.Dataset, ds);

            IReadOnlyList<Dataset> dataSets = await _fixture.Client.Datasets.List(CancellationToken.None);

            Assert.NotEmpty(dataSets);

            Assert.Contains(_fixture.Dataset, dataSets);

            string ingestData = @"[
	{
		""time"": ""17/May/2015:08:05:30 +0000"",
        ""remote_ip"": ""93.180.71.1"",
        ""remote_user"": ""-"",
        ""request"": ""GET /downloads/product_1 HTTP/1.1"",
        ""response"": 304,
        ""bytes"": 0,
        ""referrer"": ""-"",
        ""agent"": ""Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21)""
    },
    {
        ""time"": ""17/May/2015:08:05:31 +0000"",
        ""remote_ip"": ""93.180.71.2"",
        ""remote_user"": ""-"",
        ""request"": ""GET /downloads/product_1 HTTP/1.1"",
        ""response"": 304,
        ""bytes"": 0,
        ""referrer"": ""-"",
        ""agent"": ""Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21)""
    }
]";
            await using (Stream s = GetStream(ingestData))
            {
                IngestStatus ingestStatus = await _fixture.Client.Datasets.Ingest(_fixture.Dataset.Id, s,
                    ContentType.Json, ContentEncoding.Identity, null, CancellationToken.None);

                Assert.Equal(2, (int)ingestStatus.Ingested);
                Assert.Equal(0, (int)ingestStatus.Failed);
                Assert.Empty(ingestStatus.Failures);
                Assert.Equal(s.Length, (int)ingestStatus.ProcessedBytes);
            }

            await using (Stream s = GetStream(ingestData))
            {
                await using MemoryStream ms = new MemoryStream();
                await using GZipStream gZipStream = new GZipStream(ms, CompressionMode.Compress);

                await s.CopyToAsync(gZipStream);
                gZipStream.Flush();
                ms.Position = 0;

                IngestStatus ingestStatus = await _fixture.Client.Datasets.Ingest(_fixture.Dataset.Id, ms,
                    ContentType.Json, ContentEncoding.Gzip, null, CancellationToken.None);

                Assert.Equal(2, (int)ingestStatus.Ingested);
                Assert.Equal(0, (int)ingestStatus.Failed);
                Assert.Empty(ingestStatus.Failures);
                Assert.Equal(s.Length, (int)ingestStatus.ProcessedBytes);
            }

            var ingestEvents = new[]
            {
                new
                {
                    time = "17/May/2015:08:05:32 +0000",
                    remote_ip = "93.180.71.1",
                    remote_user = "-",
                    request = "GET /downloads/product_1 HTTP/1.1",
                    response = 304,
                    bytes = 0,
                    referrer = "-",
                    agent = "Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21)"
                },
                new
                {
                    time = "17/May/2015:08:05:33 +0000",
                    remote_ip = "93.180.71.2",
                    remote_user = "-",
                    request = "GET /downloads/product_1 HTTP/1.1",
                    response = 304,
                    bytes = 0,
                    referrer = "-",
                    agent = "Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21)"
                }
            };

            IngestStatus ingestEventsStatus = await _fixture.Client.Datasets.IngestEvents(_fixture.Dataset.Id, ingestEvents, null, CancellationToken.None);

            Assert.Equal(2, (int)ingestEventsStatus.Ingested);
            Assert.Equal(0, (int)ingestEventsStatus.Failed);
            Assert.Empty(ingestEventsStatus.Failures);

            DatasetInfo info = await _fixture.Client.Datasets.Info(_fixture.Dataset.Id, CancellationToken.None);
            Assert.NotNull(info);
            Assert.Equal(_fixture.Dataset.Name, info.Name);
            Assert.Equal(6, info.NumEvents);
            Assert.NotEmpty(info.Fields);

            DatasetStats stats = await _fixture.Client.Datasets.Stats(CancellationToken.None);

            Assert.NotNull(stats);
            bool datasetFound = stats.Datasets.Any(x => string.CompareOrdinal(x.Name, _fixture.Dataset.Name) == 0);
            Assert.True(datasetFound, "stats do not contain the dataset created for this test");

            string fieldDescription = "HTTP status code returned as part of the response";
            FieldUpdateRequest fieldUpdateRequest = new FieldUpdateRequest()
            {
                Description = fieldDescription
            };
            Field field = await _fixture.Client.Datasets.UpdateField(_fixture.Dataset.Id, "response",
                fieldUpdateRequest, CancellationToken.None);

            Assert.NotNull(field);
            Assert.Equal("response", field.Name);
            Assert.Equal(fieldDescription, field.Description);
            Assert.Equal("integer", field.Type);

            DatasetTrimResult trimResult = await _fixture.Client.Datasets.Trim(_fixture.Dataset.Id, TimeSpan.FromSeconds(1.0),
                CancellationToken.None);

            Assert.NotNull(trimResult);
            Assert.Equal(0, trimResult.BlocksDeleted);

        }
    }
}
