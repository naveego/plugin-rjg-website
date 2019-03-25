using Newtonsoft.Json;

namespace PluginRJGWebsite.DataContracts
{
    public class ReadRecordObject
    {
        [JsonProperty("data")]
        public object Data { get; set; }
    }
}