using Newtonsoft.Json;

namespace PluginRJGWebsite.DataContracts
{
    public class TokenResponse
    {
        [JsonProperty("token")]
        public string Token { get; set; }
        
        [JsonProperty("user_display_name")]
        public string UserDisplayName { get; set; }
        
        [JsonProperty("user_email")]
        public string UserEmail { get; set; }
        
        [JsonProperty("user_nicename")]
        public string UserNicename { get; set; }
    }
}