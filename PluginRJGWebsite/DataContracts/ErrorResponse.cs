using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginRJGWebsite.DataContracts
{
    public class ErrorResponse
    {
        [JsonProperty("code")]
        public string Code { get; set; }
        
        [JsonProperty("message")]
        public string Message { get; set; }
        
        [JsonProperty("data")]
        public Dictionary<string, object> Data { get; set; }
    }
}