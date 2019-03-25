using Newtonsoft.Json;

namespace PluginRJGWebsite.DataContracts
{
    public class TokenRequest
    {
        [JsonProperty("username")]
        public string Username { get; set; }
        
        [JsonProperty("password")]
        public string Password { get; set; }
    }
}