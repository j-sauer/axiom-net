# Axiom .Net

## Introduction

Axiom .Net is a .Net library for accessing the [Axiom](https://www.axiom.co/)
API.

## Installation

You can add the library via nuget in your project folder with the following command:
```shell
dotnet add package AxiomHq.Net --version 1.0.0-beta1
```

## Authentication

The client is initialized with the url of the deployment and an access token
when using Axiom Selfhost or an access token and the users organization id when
using Axiom Cloud.

The access token can be a personal token retrieved from the users profile page
or an ingest token retrieved from the settings of the Axiom deployment.

The personal access token grants access to all resources available to the user
on his behalf.

The ingest token just allows ingestion into the datasets the token is configured
for.

## Usage

```csharp
using AxiomHq.Net;

HttpClient httpClient = new HttpClient(); // or use an appropriate factory
string token = "yourAccessToken";
string organizationId = "axiom";
Uri baseUrl = new Uri("http://localhost:8080/axiom/");
Client client = new Client(httpCLient, baseUrl, token, organizationId);

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

var ingestEventsStatus = await client.Datasets.IngestEvents("dataSetId", ingestEvents, null, CancellationToken.None);
```

## Documentation

You can find the Axiom documentation on the [docs website.](https://docs.axiom.co/)

## Contributing

TODO

## License

&copy; Axiom, Inc., 2022

Distributed under MIT License (`The MIT License`).

See [LICENSE.md](LICENSE) for more information.
