using Newtonsoft.Json;

namespace PluginRJGWebsite.DataContracts
{
    public class ClassesPostObject
    {
        [JsonProperty("open_seats")] public int OpenSeats { get; set; }

        [JsonProperty("language")] public string Language { get; set; }

        // [JsonProperty("location_name")]
        // public string Location { get; set; }

        [JsonProperty("location_city")] public string City { get; set; }

        [JsonProperty("location_state")] public string State { get; set; }

        [JsonProperty("start_date")] public string StartDate { get; set; }

        [JsonProperty("end_date")] public string EndDate { get; set; }

        [JsonProperty("sku")] public string SKU { get; set; }

        [JsonProperty("course_sku")] public string CourseSKU { get; set; }

        [JsonProperty("price")] public string Price { get; set; }

        [JsonProperty("visible")] public bool Visible { get; set; }

        [JsonProperty("currency")] public string Currency { get; set; }

        [JsonProperty("affiliation")] public string Affiliation { get; set; }

        [JsonProperty("startdatum")] public string StartDatum { get; set; }

        [JsonProperty("enddatum")] public string EndDatum { get; set; }
        
        [JsonProperty("external")] public string External { get; set; }
    }
}