using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace PluginRJGWebsite.Helper
{
    public class RequestHelper
    {
        private readonly Authenticator _authenticator;
        private readonly HttpClient _client;
        private readonly Settings _settings;
        private readonly EnvironmentHelper _envConfig;
        
        public RequestHelper(Settings settings, HttpClient client)
        {
            _authenticator = new Authenticator(settings, client);
            _client = client;
            _settings = settings;
            _envConfig = new EnvironmentHelper(settings.Environment);
        }

        /// <summary>
        /// Get Async request wrapper for making authenticated requests
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public async Task<HttpResponseMessage> GetAsync(string path)
        {
            string token;

            // get the token
            try
            {
                token = await _authenticator.GetToken();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            // add token to the request and execute the request
            try
            {
                var uri = String.Format("{0}/{1}", _envConfig.Endpoint, path.TrimStart('/'));
                
                var client = _client;
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

                var response = await client.GetAsync(uri);

                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }
        
        /// <summary>
        /// Post Async request wrapper for making authenticated requests
        /// </summary>
        /// <param name="path"></param>
        /// <param name="json"></param>
        /// <returns></returns>
        public async Task<HttpResponseMessage> PostAsync(string path, StringContent json)
        {
            string token;

            // get the token
            try
            {
                token = await _authenticator.GetToken();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            // add token to the request and execute the request
            try
            {
                var uri = String.Format("{0}/{1}", _envConfig.Endpoint, path.TrimStart('/'));
                
                var client = _client;
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var response = await client.PostAsync(uri, json);

                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }

        /// <summary>
        /// Put Async request wrapper for making authenticated requests
        /// </summary>
        /// <param name="path"></param>
        /// <param name="json"></param>
        /// <returns></returns>
        public async Task<HttpResponseMessage> PutAsync(string path, StringContent json)
        {
            string token;

            // get the token
            try
            {
                token = await _authenticator.GetToken();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            // add token to the request and execute the request
            try
            {
                var uri = String.Format("{0}/{1}", _envConfig.Endpoint, path.TrimStart('/'));
                
                var client = _client;
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var response = await client.PutAsync(uri, json);

                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }
        
        /// <summary>
        /// Patch async wrapper for making authenticated requests
        /// </summary>
        /// <param name="path"></param>
        /// <param name="json"></param>
        /// <returns></returns>
        public async Task<HttpResponseMessage> PatchAsync(string path, StringContent json)
        {
            string token;

            // get the token
            try
            {
                token = await _authenticator.GetToken();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            // add token to the request and execute the request
            try
            {
                var uri = String.Format("{0}/{1}", _envConfig.Endpoint, path.TrimStart('/'));
                
                var client = _client;
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var response = await client.PatchAsync(uri, json);

                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }
        
        /// <summary>
        /// Delete async wrapper for making authenticated requests
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public async Task<HttpResponseMessage> DeleteAsync(string path)
        {
            string token;

            // get the token
            try
            {
                token = await _authenticator.GetToken();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            // add token to the request and execute the request
            try
            {
                var uri = String.Format("{0}/{1}", _envConfig.Endpoint, path.TrimStart('/'));
                
                var client = _client;
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

                var response = await client.DeleteAsync(uri);

                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }
    }
}