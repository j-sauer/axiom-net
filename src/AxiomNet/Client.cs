using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace AxiomNet
{
    /// <summary>
    /// The exception that is thrown when an error occurred during a request to the Axiom HTTP API.
    /// </summary>
    public class AxiomException : Exception
    {
        /// <summary>
        /// The HTTP status code.
        /// </summary>
        public int Status { get; }

        public AxiomException(int status, string message) : base(message)
        {
            Status = status;
        }

        public AxiomException(int status, string message, Exception innerException)
            : base(message, innerException)
        {
            Status = status;
        }
    }

    internal class AxiomError
    {
        public int Status { get; set; }
        [JsonPropertyName("message")]
        public string Message { get; set; }
    }

    /// <summary>
    /// A client that provides access to the Axiom HTTP API.
    /// </summary>
    public class Client
    {
        private const string CloudUrl = "https://cloud.axiom.co";
        private const string UserAgent = "axiom-net";

        private readonly HttpClient _client;

        private readonly Uri _baseUrl;
        private readonly string _accessToken;
        private readonly string? _organisationId;

        private readonly Datasets _datasets;

        /// <summary>
        /// Creates a new client. It automatically takes its configuration from the environment if the parameters beside
        /// <code>client</code> are not set.
        ///
        /// To connect to Axiom Cloud:
        ///  - AXIOM_TOKEN
        ///  - AXIOM_ORG_ID (only when using a personal token)
        ///
        /// To connect to an Axiom Selfhost:
        ///  - AXIOM_URL
        ///  - AXIOM_TOKEN
        ///
        /// The access token must be an api or personal token which can created on the settings or user profile on Axiom.
        /// </summary>
        /// <param name="client">An <code>HttpClient</code> to use for the requests.</param>
        /// <param name="baseUrl">The base url of the Axiom instance.</param>
        /// <param name="accessToken">The access token for the Axiom API.</param>
        /// <param name="organisationId">The organisation Id.</param>
        public Client(HttpClient client, Uri? baseUrl = null, string? accessToken = null, string? organisationId = null)
        {
            _client = client;

            string? deploymentUrlEnv = Environment.GetEnvironmentVariable("AXIOM_URL");
            string? accessTokenEnv = Environment.GetEnvironmentVariable("AXIOM_TOKEN");
            string? orgIdEnv = Environment.GetEnvironmentVariable("AXIOM_ORG_ID");

            if (baseUrl == null)
            {
                if (string.IsNullOrWhiteSpace(deploymentUrlEnv))
                    deploymentUrlEnv = CloudUrl;
                baseUrl = new Uri(deploymentUrlEnv);
            }

            if (accessToken == null)
            {
                if (string.IsNullOrEmpty(accessTokenEnv))
                    throw new ArgumentException(
                        $"Either {nameof(accessToken)} has to be set or environment variable AXIOM_TOKEN.");

                accessToken = accessTokenEnv;
            }

            if (!IsValidToken(accessToken))
                throw new ArgumentException(
                    $"Either {nameof(accessToken)} or environment variable AXIOM_TOKEN has the wrong format.");

            bool cloudUrlSetByOption = string.CompareOrdinal(baseUrl.ToString(), CloudUrl) == 0;
            bool cloudUrlSetByEnvironment = string.CompareOrdinal(deploymentUrlEnv, CloudUrl) == 0;
            bool isPersonalToken = IsPersonalToken(accessToken);

            if (organisationId == null)
            {
                if (orgIdEnv == null && (cloudUrlSetByOption || cloudUrlSetByEnvironment) && isPersonalToken)
                {
                    throw new ArgumentException(
                        $"Either {nameof(organisationId)} has to be set or environment variable AXIOM_ORG_ID.");
                }

                organisationId = orgIdEnv;
            }

            _baseUrl = baseUrl;
            _accessToken = accessToken;
            _organisationId = organisationId;

            _datasets = new Datasets(this);
        }

        /// <summary>
        /// Provides access to the dataset operations of the Axiom HTTP API.
        /// </summary>
        public Datasets Datasets => _datasets;

        private bool IsApiToken(string token) => token.StartsWith("xaat-");
        private bool IsIngestToken(string token) => token.StartsWith("xait-");
        private bool IsPersonalToken(string token) => token.StartsWith("xapt-");

        private bool IsValidToken(string token) => IsApiToken(token) || IsIngestToken(token) || IsPersonalToken(token);

        private void ValidateCredentials()
        {

        }

        internal async Task Call(HttpMethod method, string? path, CancellationToken ct)
        {
            HttpRequestMessage req = CreateNewRequest(method, path);

            await Do(req, ct);
        }

        internal async Task<TResult> Call<TResult>(HttpMethod method, string? path, CancellationToken ct)
        {
            HttpRequestMessage req = CreateNewRequest(method, path);

            return await Do<TResult>(req, ct);
        }

        internal async Task<TResult> Call<TResult, TBody>(HttpMethod method, string? path, TBody body, CancellationToken ct)
        {
            HttpRequestMessage req = CreateNewRequest(method, path);

            req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            req.Content = JsonContent.Create(body);

            return await Do<TResult>(req, ct);
        }

        internal async Task<TResult> Call<TResult>(HttpMethod method, string? path, Stream body, CancellationToken ct)
        {
            HttpRequestMessage req = CreateNewRequest(method, path, body);

            return await Do<TResult>(req, ct);
        }

        internal HttpRequestMessage CreateNewRequest(HttpMethod method, string? path, Stream s)
        {
            HttpRequestMessage req = CreateNewRequest(method, path);

            req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            req.Content = new StreamContent(s);

            return req;
        }

        private HttpRequestMessage CreateNewRequest(HttpMethod method, string? path)
        {
            Uri uri = path != null ? new Uri(_baseUrl, path) : _baseUrl;
            HttpRequestMessage req = new HttpRequestMessage(method, uri);

            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);

            if (!IsIngestToken(_accessToken) && _organisationId != null)
            {
                req.Headers.Add("X-Axiom-Org-Id", _organisationId);
            }
            req.Headers.UserAgent.Add(new ProductInfoHeaderValue(new ProductHeaderValue(UserAgent)));

            return req;
        }

        private async Task HandleErrorStatus(HttpResponseMessage resp)
        {
            if ((int)resp.StatusCode >= 400)
            {
                if (resp.Content == null || resp.Content.Headers.ContentType.MediaType != "application/json")
                {
                    throw new AxiomException((int)resp.StatusCode, resp.ReasonPhrase);
                }

                string content = await resp.Content.ReadAsStringAsync();

                AxiomError? err = null;
                try
                {
                    err = JsonSerializer.Deserialize<AxiomError>(content);
                }
                catch { }
                if (err != null)
                    throw new AxiomException((int)resp.StatusCode, err.Message);

                throw new AxiomException((int)resp.StatusCode, content);
            }
        }

        internal async Task Do(HttpRequestMessage req, CancellationToken ct)
        {
            HttpResponseMessage resp = await _client.SendAsync(req, ct);

            await HandleErrorStatus(resp);
        }

        internal async Task<T> Do<T>(HttpRequestMessage req, CancellationToken ct)
        {
            HttpResponseMessage resp = await _client.SendAsync(req, ct);

            await HandleErrorStatus(resp);

            string content = await resp.Content.ReadAsStringAsync();

            T? res = JsonSerializer.Deserialize<T>(content);

            if (res == null)
                throw new AxiomException((int)resp.StatusCode, "Response is empty.");

            return res;
        }
    }
}
