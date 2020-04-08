using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginRJGWebsite.DataContracts;
using PluginRJGWebsite.Helper;

namespace PluginRJGWebsite.Read
{
    public class PrepareRecords
    {
        public static Dictionary<string, object> PrepareRecordTypes(Dictionary<string, object> record, Schema schema,
            Endpoint endpoint)
        {
            foreach (var property in schema.Properties)
            {
                if (record.ContainsKey(property.Id))
                {
                    object value;
                    switch (property.Type)
                    {
                        case PropertyType.String:
                            value = record[property.Id];
                            if (!(value is string))
                            {
                                record[property.Id] = JsonConvert.SerializeObject(value);
                            }

                            break;
                        case PropertyType.Json:
                            value = record[property.Id];
                            record[property.Id] = new ReadRecordObject
                            {
                                Data = value ?? new Dictionary<string, object>()
                            };
                            break;
                        case PropertyType.Datetime:
                            if (record[property.Id] != null)
                            {
                                if (DateTime.TryParse(record[property.Id].ToString(), out var date))
                                {
                                    record[property.Id] = date.ToString("O", CultureInfo.InvariantCulture);
                                }
                            }

                            break;
                    }

                    record = CustomProcessing(record, property, endpoint);
                }
            }
            
            return record;
        }

        private static Dictionary<string, object> CustomProcessing(Dictionary<string, object> record, Property property,
            Endpoint endpoint)
        {
            if (endpoint.Name == "Assessments")
            {
                var metric = Regex.Split(property.Id, "-metric-", RegexOptions.IgnoreCase);

                if (metric.Length > 1)
                {
                    if (!String.IsNullOrEmpty(record[property.Id].ToString()))
                    {
                        var stdId = property.Id.Replace("metric-", "");
                        record[stdId] = record[property.Id];
                    }
                }
            }

            return record;
        }
    }
}