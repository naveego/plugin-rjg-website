using Newtonsoft.Json;

namespace PluginRJGWebsite.DataContracts
{
    public class Field
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        
        [JsonProperty("field_key")]
        public string FieldKey { get; set; }
        
        [JsonProperty("name")]
        public string Name { get; set; }
        
        [JsonProperty("description")]
        public string Description { get; set; }
        
        [JsonProperty("type")]
        public string Type { get; set; }
        
        [JsonProperty("options")]
        public object Options { get; set; }
        
        [JsonProperty("required")]
        public string Required { get; set; }
    }
}