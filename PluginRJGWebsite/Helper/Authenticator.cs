using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using PluginRJGWebsite.DataContracts;

namespace PluginRJGWebsite.Helper
{
    public class Authenticator
    {
        private readonly HttpClient _client;
        private readonly Settings _settings;
        private string _token;

        public Authenticator(Settings settings, HttpClient client)
        {
            _client = client;
            _settings = settings;
            _token = String.Empty;
        }

        /// <summary>
        /// Get a token for the Zoho API
        /// </summary>
        /// <returns></returns>
        public async Task<string> GetToken()
        {
            // check if token is present
            if (String.IsNullOrEmpty(_token))
            {
                return await GetNewToken();
            }
            // return saved token
            else
            {
                return await ValidateToken();
            }
        }

        private async Task<string> GetNewToken()
        {
            try
            {
                // get a token
                var envConfig = new EnvironmentHelper(_settings.Environment);

                var uri = $"{envConfig.Endpoint}/jwt-auth/v1/token";

                var tokenRequest = new TokenRequest
                {
                    Username = _settings.Username,
                    Password = _settings.Password
                };
                var json = new StringContent(JsonConvert.SerializeObject(tokenRequest), Encoding.UTF8,
                    "application/json");

                var client = _client;
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var response = await _client.PostAsync(uri, json);
                response.EnsureSuccessStatusCode();

                var content = JsonConvert.DeserializeObject<TokenResponse>(await response.Content.ReadAsStringAsync());

                // update saved token
                _token = content.Token;

                return _token;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }

        private async Task<string> ValidateToken()
        {
            try
            {
                // check if token is still valid
                var envConfig = new EnvironmentHelper(_settings.Environment);

                var uri = $"{envConfig.Endpoint}/jwt-auth/v1/token/validate";

                var client = _client;
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _token);

                var response = await _client.PostAsync(uri, null);
                response.EnsureSuccessStatusCode();

                return _token;
            }
            catch
            {
                // get new token if not valid
                return await GetNewToken();
            }
        }
    }
}