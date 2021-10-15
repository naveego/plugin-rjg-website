using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginRJGWebsite.DataContracts;
using PluginRJGWebsite.Helper;

namespace PluginRJGWebsite.Write
{
    public static class WriteRecords
    {
        /// <summary>
        /// Writes a record out to RJG Website
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="record"></param>
        /// <param name="endpointHelper"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        public static async Task<string> PutRecord(Schema schema, Record record, EndpointHelper endpointHelper,
            RequestHelper client)
        {
            // Logger.SetLogLevel(Logger.LogLevel.Debug);
            Dictionary<string, object> recObj;
            var endpoint = endpointHelper.GetEndpointForName(schema.Id);

            if (String.IsNullOrEmpty(endpoint.MetaDataPath))
            {
                try
                {
                    recObj = JsonConvert.DeserializeObject<Dictionary<string, object>>(record.DataJson);

                    Logger.Debug($"Raw: {JsonConvert.SerializeObject(recObj, Formatting.Indented)}");

                    var postObj = GetPostObject(endpoint, recObj);

                    Logger.Debug($"Post Obj: {JsonConvert.SerializeObject(postObj, Formatting.Indented)}");

                    var content = new StringContent(JsonConvert.SerializeObject(postObj), Encoding.UTF8,
                        "application/json");

                    var response = await client.PostAsync(endpoint.ReadPaths.First(), content);

                    // add checking for if patch needs to happen
                    if (!response.IsSuccessStatusCode)
                    {
                        Logger.Debug(await response.Content.ReadAsStringAsync());
                        var errorResponse =
                            JsonConvert.DeserializeObject<ErrorResponse>(await response.Content.ReadAsStringAsync());

                        Logger.Debug($"Post response: {await response.Content.ReadAsStringAsync()}");

                        if (errorResponse.Code == "product_invalid_sku" && errorResponse.Message.Contains("duplicated"))
                        {
                            // record already exists, check date then patch it
                            var id = errorResponse.Data["resource_id"];

                            // build and send request
                            var path = String.Format("{0}/{1}", endpoint.ReadPaths.First(), id);

                            var patchObj = GetPatchObject(endpoint, recObj);

                            Logger.Debug($"Patch Obj: {JsonConvert.SerializeObject(patchObj, Formatting.Indented)}");

                            content = new StringContent(JsonConvert.SerializeObject(patchObj), Encoding.UTF8,
                                "application/json");

                            response = await client.PatchAsync(path, content);
                            Logger.Debug($"Patch response: {await response.Content.ReadAsStringAsync()}");
                            Logger.Debug(await response.Content.ReadAsStringAsync());

                            if (!response.IsSuccessStatusCode)
                            {
                                Logger.Error(null, "Failed to update record.");
                                return await response.Content.ReadAsStringAsync();
                            }

                            Logger.Info("Modified 1 record.");
                            return "";
                        }

                        Logger.Error(null, "Failed to create record.");
                        return await response.Content.ReadAsStringAsync();
                    }

                    Logger.Info("Created 1 record.");
                    return "";
                }
                catch (AggregateException e)
                {
                    Logger.Error(e, e.Flatten().ToString());
                    return e.Message;
                }
            }

            // code for modifying forms would go here if needed but currently is not needed

            return "Write backs are only supported for Classes.";
        }
        
        /// <summary>
        /// Deletes a record from the RJG Website
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="record"></param>
        /// <param name="endpointHelper"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        public static async Task<string> DeleteRecord(Schema schema, Record record, EndpointHelper endpointHelper,
            RequestHelper client)
        {
            Dictionary<string, object> recObj;
            var endpoint = endpointHelper.GetEndpointForName(schema.Id);

            if (String.IsNullOrEmpty(endpoint.MetaDataPath))
            {
                try
                {
                    recObj = JsonConvert.DeserializeObject<Dictionary<string, object>>(record.DataJson);

                    if (recObj.ContainsKey("id"))
                    {
                        if (recObj["id"] != null)
                        {
                            // delete record
                            // try each endpoint
                            foreach (var path in endpoint.ReadPaths)
                            {
                                try
                                {
                                    var uri = String.Format("{0}/{1}", path, recObj["id"]);
                                    var response = await client.DeleteAsync(uri);
                                    response.EnsureSuccessStatusCode();

                                    Logger.Info("Deleted 1 record.");
                                    return "";
                                }
                                catch (Exception e)
                                {
                                    Logger.Error(e, e.Message);
                                }
                            }
                        }

                        return "Could not delete record with no id.";
                    }

                    return "Key 'id' not found on requested record to delete.";
                }
                catch (Exception e)
                {
                    Logger.Error(e, e.Message);
                    return e.Message;
                }
            }

            // code for modifying forms would go here if needed but currently is not needed

            return "Write backs are only supported for Classes.";
        }

        /// <summary>
        /// Gets the object to write out to the endpoint
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="recObj"></param>
        /// <returns></returns>
        private static object GetPatchObject(Endpoint endpoint, Dictionary<string, object> recObj)
        {
            switch (endpoint.Name)
            {
                case "Classes - Write":
                    if (recObj.TryGetValue("open_seats", out var openSeats))
                    {
                        if (openSeats == null)
                        {
                            openSeats = 0;
                        }
                    }
                    else
                    {
                        openSeats = 0;
                    }

                    if (recObj.TryGetValue("language", out var language))
                    {
                        if (language == null)
                        {
                            language = "";
                        }
                    }
                    else
                    {
                        language = "";
                    }

                    // if (recObj.TryGetValue("location_name", out var location))
                    // {
                    //     if (location == null)
                    //     {
                    //         location = "";
                    //     }
                    // }
                    // else
                    // {
                    //     location = "";
                    // }

                    if (recObj.TryGetValue("location_city", out var city))
                    {
                        if (city == null)
                        {
                            city = "";
                        }
                    }
                    else
                    {
                        city = "";
                    }

                    if (recObj.TryGetValue("location_state", out var state))
                    {
                        if (state == null)
                        {
                            if (recObj.TryGetValue("location_state_province_county", out state))
                            {
                                if (state == null)
                                {
                                    state = "";
                                }
                            }
                            else
                            {
                                state = "";
                            }
                        }
                    }
                    else
                    {
                        if (recObj.TryGetValue("location_state_province_county", out state))
                        {
                            if (state == null)
                            {
                                state = "";
                            }
                        }
                        else
                        {
                            state = "";
                        }
                    }

                    if (recObj.TryGetValue("start_date", out var startDate))
                    {
                        if (startDate != null)
                        {
                            startDate = DateTime.Parse(startDate.ToString())
                                .ToString("yyyy/MM/dd", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            startDate = "";
                        }
                    }
                    else
                    {
                        startDate = "";
                    }

                    if (recObj.TryGetValue("end_date", out var endDate))
                    {
                        if (endDate != null)
                        {
                            endDate = DateTime.Parse(endDate.ToString())
                                .ToString("yyyy/MM/dd", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            endDate = "";
                        }
                    }
                    else
                    {
                        endDate = "";
                    }

                    if (recObj.TryGetValue("course_sku", out var courseSku))
                    {
                        if (courseSku == null)
                        {
                            courseSku = "";
                        }
                    }
                    else
                    {
                        courseSku = "";
                    }

                    if (recObj.TryGetValue("price", out var price))
                    {
                        if (price == null)
                        {
                            price = "";
                        }
                    }
                    else
                    {
                        price = "";
                    }

                    if (recObj.TryGetValue("visible", out var visible))
                    {
                        if (visible == null)
                        {
                            visible = true;
                        }
                    }
                    else
                    {
                        visible = true;
                    }

                    if (recObj.TryGetValue("currency", out var currency))
                    {
                        if (currency == null)
                        {
                            currency = "";
                        }
                    }
                    else
                    {
                        currency = "";
                    }

                    if (recObj.TryGetValue("affiliation", out var affiliation))
                    {
                        if (affiliation == null)
                        {
                            affiliation = "";
                        }
                    }
                    else
                    {
                        affiliation = "";
                    }

                    if (recObj.TryGetValue("startdatum", out var startDatum))
                    {
                        if (startDatum != null)
                        {
                            startDatum = DateTime.Parse(startDatum.ToString())
                                .ToString("yyyy/MM/dd", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            startDatum = "";
                        }
                    }
                    else
                    {
                        startDatum = "";
                    }

                    if (recObj.TryGetValue("enddatum", out var endDatum))
                    {
                        if (endDatum != null)
                        {
                            endDatum = DateTime.Parse(endDatum.ToString())
                                .ToString("yyyy/MM/dd", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            endDatum = "";
                        }
                    }
                    else
                    {
                        endDatum = "";
                    }
                    
                    if (recObj.TryGetValue("external", out var external))
                    {
                        if (external == null)
                        {
                            external = "";
                        }
                    }
                    else
                    {
                        external = "";
                    }

                    return new ClassesPatchObject
                    {
                        OpenSeats = int.Parse(openSeats.ToString()),
                        Language = language.ToString(),
                        // Location = location.ToString(),
                        City = city.ToString(),
                        State = state.ToString(),
                        StartDate = startDate.ToString(),
                        EndDate = endDate.ToString(),
                        CourseSKU = courseSku.ToString(),
                        Price = price.ToString(),
                        Visible = bool.Parse(visible.ToString()),
                        Currency = currency.ToString(),
                        Affiliation = affiliation.ToString(),
                        StartDatum = startDatum.ToString(),
                        EndDatum = endDatum.ToString(),
                        External = external.ToString()
                    };
                default:
                    return new object();
            }
        }

        /// <summary>
        /// Gets the object to write out to the endpoint
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="recObj"></param>
        /// <returns></returns>
        private static object GetPostObject(Endpoint endpoint, Dictionary<string, object> recObj)
        {
            switch (endpoint.Name)
            {
                case "Classes - Write":
                    if (recObj.TryGetValue("open_seats", out var openSeats))
                    {
                        if (openSeats == null)
                        {
                            openSeats = 0;
                        }
                    }
                    else
                    {
                        openSeats = 0;
                    }

                    if (recObj.TryGetValue("language", out var language))
                    {
                        if (language == null)
                        {
                            language = "";
                        }
                    }
                    else
                    {
                        language = "";
                    }

                    // if (recObj.TryGetValue("location_name", out var location))
                    // {
                    //     if (location == null)
                    //     {
                    //         location = "";
                    //     }
                    // }
                    // else
                    // {
                    //     location = "";
                    // }

                    if (recObj.TryGetValue("location_city", out var city))
                    {
                        if (city == null)
                        {
                            city = "";
                        }
                    }
                    else
                    {
                        city = "";
                    }

                    if (recObj.TryGetValue("location_state", out var state))
                    {
                        if (state == null)
                        {
                            if (recObj.TryGetValue("location_state_province_county", out state))
                            {
                                if (state == null)
                                {
                                    state = "";
                                }
                            }
                            else
                            {
                                state = "";
                            }
                        }
                    }
                    else
                    {
                        if (recObj.TryGetValue("location_state_province_county", out state))
                        {
                            if (state == null)
                            {
                                state = "";
                            }
                        }
                        else
                        {
                            state = "";
                        }
                    }

                    if (recObj.TryGetValue("start_date", out var startDate))
                    {
                        if (startDate != null)
                        {
                            startDate = DateTime.Parse(startDate.ToString())
                                .ToString("yyyy/MM/dd", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            startDate = "";
                        }
                    }
                    else
                    {
                        startDate = "";
                    }

                    if (recObj.TryGetValue("end_date", out var endDate))
                    {
                        if (endDate != null)
                        {
                            endDate = DateTime.Parse(endDate.ToString())
                                .ToString("yyyy/MM/dd", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            endDate = "";
                        }
                    }
                    else
                    {
                        endDate = "";
                    }

                    if (recObj.TryGetValue("sku", out var sku))
                    {
                        if (sku == null)
                        {
                            sku = "";
                        }
                    }
                    else
                    {
                        sku = "";
                    }

                    if (recObj.TryGetValue("course_sku", out var courseSku))
                    {
                        if (courseSku == null)
                        {
                            courseSku = "";
                        }
                    }
                    else
                    {
                        courseSku = "";
                    }

                    if (recObj.TryGetValue("price", out var price))
                    {
                        if (price == null)
                        {
                            price = "";
                        }
                    }
                    else
                    {
                        price = "";
                    }

                    if (recObj.TryGetValue("visible", out var visible))
                    {
                        if (visible == null)
                        {
                            visible = true;
                        }
                    }
                    else
                    {
                        visible = true;
                    }

                    if (recObj.TryGetValue("currency", out var currency))
                    {
                        if (currency == null)
                        {
                            currency = "";
                        }
                    }
                    else
                    {
                        currency = "";
                    }

                    if (recObj.TryGetValue("affiliation", out var affiliation))
                    {
                        if (affiliation == null)
                        {
                            affiliation = "";
                        }
                    }
                    else
                    {
                        affiliation = "";
                    }

                    if (recObj.TryGetValue("startdatum", out var startDatum))
                    {
                        if (startDatum != null)
                        {
                            startDatum = DateTime.Parse(startDatum.ToString())
                                .ToString("yyyy/MM/dd", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            startDatum = "";
                        }
                    }
                    else
                    {
                        startDatum = "";
                    }

                    if (recObj.TryGetValue("enddatum", out var endDatum))
                    {
                        if (endDatum != null)
                        {
                            endDatum = DateTime.Parse(endDatum.ToString())
                                .ToString("yyyy/MM/dd", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            endDatum = "";
                        }
                    }
                    else
                    {
                        endDatum = "";
                    }
                    
                    if (recObj.TryGetValue("external", out var external))
                    {
                        if (external == null)
                        {
                            external = "";
                        }
                    }
                    else
                    {
                        external = "";
                    }

                    return new ClassesPostObject
                    {
                        OpenSeats = int.Parse(openSeats.ToString()),
                        Language = language.ToString(),
                        // Location = location.ToString(),
                        City = city.ToString(),
                        State = state.ToString(),
                        StartDate = startDate.ToString(),
                        EndDate = endDate.ToString(),
                        SKU = sku.ToString(),
                        CourseSKU = courseSku.ToString(),
                        Price = price.ToString(),
                        Visible = bool.Parse(visible.ToString()),
                        Currency = currency.ToString(),
                        Affiliation = affiliation.ToString(),
                        StartDatum = startDatum.ToString(),
                        EndDatum = endDatum.ToString(),
                        External = external.ToString()
                    };
                default:
                    return new object();
            }
        }
    }
}