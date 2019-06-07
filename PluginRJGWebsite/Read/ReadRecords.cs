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
        public static async Task<List<Dictionary<string, object>>> GetAllRecords(RequestHelper client, Endpoint endpoint)
        {
            var records = new List<Dictionary<string, object>>();
            if (!String.IsNullOrEmpty(endpoint.MetaDataPath))
            {
                var tasks = endpoint.ReadPaths.Select(p => ReadRecords.GetRecordsHasMetaDataPath(p, client, endpoint))
                    .ToArray();

                await Task.WhenAll(tasks);

                records.AddRange(tasks.SelectMany(x => x.Result).ToList());
            }
            else
            {
                var tasks = endpoint.ReadPaths.Select(p => ReadRecords.GetRecordsNoMetaDataPath(p, client, endpoint))
                    .ToArray();

                await Task.WhenAll(tasks);

                records.AddRange(tasks.SelectMany(x => x.Result).ToList());
            }

            return records;
        }
        
        /// <summary>
        /// Gets all records and prepares them to be read
        /// </summary>
        /// <param name="path"></param>
        /// <param name="client"></param>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        private static async Task<List<Dictionary<string, object>>> GetRecordsHasMetaDataPath(string path, RequestHelper client, Endpoint endpoint)
        {
            var records = new List<Dictionary<string, object>>();

            int page = 1;
            while (true)
            {
                try
                {
                    var response = await client.GetAsync($"{path}?page={page}");
                    response.EnsureSuccessStatusCode();

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
                        records.Add(customRecord);

                        records.Add(customRecord);
                    }

                    page++;
                }
                catch
                {
                    return records;
                }
            }
        }

        /// <summary>
        /// Gets all records
        /// </summary>
        /// <param name="path"></param>
        /// <param name="client"></param>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        private static async Task<List<Dictionary<string, object>>> GetRecordsNoMetaDataPath(string path, RequestHelper client, Endpoint endpoint)
        {
            var records = new List<Dictionary<string, object>>();
            try
            {
                int page = 1;
                bool morePages = true;

                do
                {
                    var response = await client.GetAsync($"{path}?page={page}");
                    response.EnsureSuccessStatusCode();

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
                        records.Add(customRecord);
                    }
                } while (morePages);

                return records;
            }
            catch
            {
                Logger.Info($"No records for path {path}");
                return records;
            }
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