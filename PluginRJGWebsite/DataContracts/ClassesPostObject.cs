using Newtonsoft.Json;

namespace PluginRJGWebsite.DataContracts
{
    public class ClassesPostObject
    {
        [JsonProperty("open_seats")]
        public int OpenSeats { get; set; }
        
        [JsonProperty("language")]
        public string Language { get; set; }
        
        [JsonProperty("location")]
        public string Location { get; set; }
        
        [JsonProperty("start_date")]
        public string StartDate { get; set; }
        
        [JsonProperty("end_date")]
        public string EndDate { get; set; }
        
        [JsonProperty("sku")]
        public string SKU { get; set; }
        
        [JsonProperty("course_sku")]
        public string CourseSKU { get; set; }
        
        [JsonProperty("price")]
        public string Price { get; set; }
    }
}