using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using PluginRJGWebsite.Helper;

namespace PluginRJGWebsite.Read
{
    public static class ReadRecords
    {
        /// <summary>
        /// Gets all records
        /// </summary>
        /// <param name="client"></param>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        public static async IAsyncEnumerable<Dictionary<string, object>> GetAllRecords(RequestHelper client, Endpoint endpoint)
        {
            if (!String.IsNullOrEmpty(endpoint.MetaDataPath))
            {
                foreach (var path in endpoint.ReadPaths)
                {
                    var records = GetRecordsHasMetaDataPath(path, client, endpoint);
                    await foreach (var record in records)
                    {
                        yield return record;
                    }
                }
            }
            else
            {
                foreach (var path in endpoint.ReadPaths)
                {
                    var records = GetRecordsNoMetaDataPath(path, client, endpoint);
                    await foreach (var record in records)
                    {
                        yield return record;
                    }
                }
            }
        }
        
        /// <summary>
        /// Gets all records and prepares them to be read
        /// </summary>
        /// <param name="path"></param>
        /// <param name="client"></param>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        private static async IAsyncEnumerable<Dictionary<string, object>> GetRecordsHasMetaDataPath(string path, RequestHelper client, Endpoint endpoint)
        {
            int page = 1;
            while (true)
            {
                var response = await client.GetAsync($"{path}?page={page}");
                if (await response.Content.ReadAsStringAsync() == "[]")
                {
                    break;
                }

                var recordsResponse =
                    JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, object>>>(
                        await response.Content.ReadAsStringAsync());

                foreach (var record in recordsResponse)
                {
                    var outData = new Dictionary<string, object>();
                    var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(
                        JsonConvert.SerializeObject(record.Value["meta"]));

                    outData.Add("id", record.Value["id"]);

                    foreach (var field in data)
                    {
                        var result = Regex.Split(field.Key, ".*-[a-z]{2}_(.*)", RegexOptions.IgnoreCase);

                        outData.Add(result.Length > 1 ? result[1] : field.Key, field.Value);
                    }
                        
                    var customRecord = PopulateCustomFields(outData, endpoint);
                    
                    yield return customRecord;
                }

                page++;
            }
        }

        /// <summary>
        /// Gets all records
        /// </summary>
        /// <param name="path"></param>
        /// <param name="client"></param>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        private static async IAsyncEnumerable<Dictionary<string, object>> GetRecordsNoMetaDataPath(string path, RequestHelper client, Endpoint endpoint)
        {
            int page = 1;
            bool morePages = true;

            do
            {
                var response = await client.GetAsync($"{path}?page={page}");
                if (!response.IsSuccessStatusCode)
                {
                    break;
                }

                if (response.Headers.TryGetValues("Link", out var linkHeaders))
                {
                    var linkHeader = linkHeaders.FirstOrDefault();
                    if (linkHeader != null)
                    {
                        var result = Regex.Split(linkHeader, "rel=\"next\"", RegexOptions.IgnoreCase);

                        if (result.Length > 1)
                        {
                            page++;
                        }
                        else
                        {
                            morePages = false;
                        }
                    }
                    else
                    {
                        morePages = false;
                    }
                }
                else
                {
                    morePages = false;
                }

                var recordsResponse =
                    JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(
                        await response.Content.ReadAsStringAsync());

                foreach (var record in recordsResponse)
                {
                    var customRecord = PopulateCustomFields(record, endpoint);

                    yield return customRecord;
                }
            } while (morePages);
        }

        /// <summary>
        /// Populates custom fields on records
        /// </summary>
        /// <param name="record"></param>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        private static Dictionary<string, object> PopulateCustomFields(Dictionary<string, object> record, Endpoint endpoint)
        {
            if (endpoint.Name == "Assessments")
            {
                record.Add("course_assessment_name", record["id"]);
            }

            if (endpoint.Name == "Registrations")
            {
                record.Add("WordpressID", record["id"]);
            }
            
            if (endpoint.Name == "Wait List Registrations")
            {
                record.Add("WordpressID", record["id"]);
            }
            
            return record;
        }
    }
}