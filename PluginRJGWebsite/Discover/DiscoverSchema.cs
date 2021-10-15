using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using PluginRJGWebsite.DataContracts;
using PluginRJGWebsite.Helper;

namespace PluginRJGWebsite.Discover
{
  public static class DiscoverSchema
  {
    /// <summary>
    /// Gets a schema for a given endpoint
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="client"></param>
    /// <returns>returns a schema or null if unavailable</returns>
    public static async Task<Schema> GetSchemaForEndpoint(Endpoint endpoint, RequestHelper client)
    {
      // base schema to be added to
      var schema = new Schema
      {
        Id = endpoint.Name,
        Name = endpoint.Name,
        Description = endpoint.Name,
        PublisherMetaJson = JsonConvert.SerializeObject(new PublisherMetaJson
        {
        }),
        DataFlowDirection = endpoint.DataFlowDirection
      };

      // static write schema for classes
      if (endpoint.Name == "Classes - Write")
      {
        return GetClassesWrite(schema);
      }

      try
      {
        Logger.Debug($"Getting fields for: {endpoint.Name}");

        // get fields for endpoint
        if (!String.IsNullOrEmpty(endpoint.MetaDataPath))
        {
          var response = await client.GetAsync(endpoint.MetaDataPath);

          var fields =
              JsonConvert.DeserializeObject<Dictionary<string, Field>>(
                  await response.Content.ReadAsStringAsync());

          var key = new Property
          {
            Id = "id",
            Name = "id",
            Type = PropertyType.String,
            IsKey = true,
            IsCreateCounter = false,
            IsUpdateCounter = false,
            TypeAtSource = "id",
            IsNullable = false
          };
          schema.Properties.Add(key);

          var create = new Property
          {
            Id = "created_at",
            Name = "created_at",
            Type = PropertyType.Datetime,
            IsKey = false,
            IsCreateCounter = true,
            IsUpdateCounter = false,
            TypeAtSource = "created_at",
            IsNullable = false
          };
          schema.Properties.Add(create);

          var update = new Property
          {
            Id = "updated_at",
            Name = "updated_at",
            Type = PropertyType.Datetime,
            IsKey = false,
            IsCreateCounter = false,
            IsUpdateCounter = true,
            TypeAtSource = "updated_at",
            IsNullable = false
          };
          schema.Properties.Add(update);

          foreach (var fieldKey in fields.Keys)
          {
            var field = fields[fieldKey];

            var result = Regex.Split(field.FieldKey, ".*-[a-z]{2}_(.*)", RegexOptions.IgnoreCase);

            if (result.Length > 1)
            {
              var property = new Property
              {
                Id = result[1],
                Name = field.Name,
                Type = GetPropertyTypeFromField(field),
                IsKey = false,
                IsCreateCounter = false,
                IsUpdateCounter = false,
                TypeAtSource = field.Type,
                IsNullable = field.Required != "1"
              };

              schema.Properties.Add(property);
            }
            else
            {
              if (!field.Type.ToLower().Contains("divider") && !field.Type.ToLower().Contains("break"))
              {
                var property = new Property
                {
                  Id = field.FieldKey,
                  Name = field.Name,
                  Type = GetPropertyTypeFromField(field),
                  IsKey = false,
                  IsCreateCounter = false,
                  IsUpdateCounter = false,
                  TypeAtSource = field.Type,
                  IsNullable = field.Required != "1"
                };

                schema.Properties.Add(property);
              }
            }
          }
        }
        else
        {
          var response = await client.GetAsync(endpoint.ReadPaths.First());

          var recordsList =
              JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(
                  await response.Content.ReadAsStringAsync());

          var types = GetPropertyTypesFromRecords(recordsList);
          var record = recordsList.First();

          foreach (var recordKey in record.Keys)
          {
            var property = new Property
            {
              Id = recordKey,
              Name = recordKey,
              Type = types[recordKey],
              IsKey = recordKey == "id",
              IsCreateCounter = recordKey.Contains("date_created"),
              IsUpdateCounter = recordKey.Contains("date_modified"),
              TypeAtSource = "",
              IsNullable = true
            };

            schema.Properties.Add(property);
          }
        }

        // Add custom fields
        schema = AddCustomFields(schema, endpoint);

        Logger.Debug($"Added schema for: {endpoint.Name}");
        return schema;
      }
      catch (Exception e)
      {
        Logger.Error(e, $"Failed getting fields for: {endpoint.Name}");
        Logger.Error(e, e.Message);
        return null;
      }
    }

    /// <summary>
    /// Gets the Naveego type from the provided RJG information
    /// </summary>
    /// <param name="field"></param>
    /// <returns>The property type</returns>
    private static PropertyType GetPropertyTypeFromField(Field field)
    {
      switch (field.Type)
      {
        case "checkbox":
          return PropertyType.Json;
        default:
          return PropertyType.String;
      }
    }

    /// <summary>
    /// Gets the Naveego type from the provided RJG information
    /// </summary>
    /// <param name="records"></param>
    /// <returns>The property type</returns>
    private static Dictionary<string, PropertyType> GetPropertyTypesFromRecords(
        List<Dictionary<string, object>> records)
    {
      try
      {
        // build up a dictionary of the count of each type for each property
        var discoveredTypes = new Dictionary<string, Dictionary<PropertyType, int>>();

        foreach (var record in records)
        {
          foreach (var recordKey in record.Keys)
          {
            if (!discoveredTypes.ContainsKey(recordKey))
            {
              discoveredTypes.Add(recordKey, new Dictionary<PropertyType, int>
                            {
                                {PropertyType.String, 0},
                                {PropertyType.Bool, 0},
                                {PropertyType.Integer, 0},
                                {PropertyType.Float, 0},
                                {PropertyType.Json, 0},
                                {PropertyType.Datetime, 0}
                            });
            }

            var value = record[recordKey];

            if (value == null)
              continue;

            switch (value)
            {
              case bool _:
                discoveredTypes[recordKey][PropertyType.Bool]++;
                break;
              case long _:
                discoveredTypes[recordKey][PropertyType.Integer]++;
                break;
              case double _:
                discoveredTypes[recordKey][PropertyType.Float]++;
                break;
              case JToken _:
                discoveredTypes[recordKey][PropertyType.Json]++;
                break;
              default:
                {
                  if (DateTime.TryParse(value.ToString(), out DateTime d))
                  {
                    discoveredTypes[recordKey][PropertyType.Datetime]++;
                  }
                  else
                  {
                    discoveredTypes[recordKey][PropertyType.String]++;
                  }

                  break;
                }
            }
          }
        }

        // return object
        var outTypes = new Dictionary<string, PropertyType>();

        // get the most frequent type of each property
        foreach (var typesDic in discoveredTypes)
        {
          var type = typesDic.Value.First(x => x.Value == typesDic.Value.Values.Max()).Key;
          outTypes.Add(typesDic.Key, type);
        }

        return outTypes;
      }
      catch (Exception e)
      {
        Logger.Error(e, e.Message);
        throw;
      }
    }

    private static Schema AddCustomFields(Schema schema, Endpoint endpoint)
    {
      if (endpoint.Name == "Assessments")
      {
        var courseName = new Property
        {
          Id = "course_assessment_name",
          Name = "Course Assessment Name",
          Type = PropertyType.String,
          IsKey = false,
          IsCreateCounter = false,
          IsUpdateCounter = false,
          TypeAtSource = "id",
          IsNullable = false
        };

        schema.Properties.Add(courseName);
      }

      if (endpoint.Name == "Registrations")
      {
        var property = new Property
        {
          Id = "WordpressID",
          Name = "WordpressID",
          Type = PropertyType.Integer,
          IsKey = false,
          IsCreateCounter = false,
          IsUpdateCounter = false,
          TypeAtSource = "id",
          IsNullable = false
        };

        schema.Properties.Add(property);
      }

      if (endpoint.Name == "Registrations" || endpoint.Name == "Courses" || endpoint.Name == "Classes - Read")
      {
        schema.Properties.First(p => p.Id == "date_created").Type = PropertyType.Datetime;
        schema.Properties.First(p => p.Id == "date_created_gmt").Type = PropertyType.Datetime;
      }

      if (endpoint.Name == "Wait List Registrations")
      {
        var property = new Property
        {
          Id = "WordpressID",
          Name = "WordpressID",
          Type = PropertyType.String,
          IsKey = false,
          IsCreateCounter = false,
          IsUpdateCounter = false,
          TypeAtSource = "id",
          IsNullable = false
        };

        schema.Properties.Add(property);

        property = new Property
        {
          Id = "jjjd6-value",
          Name = "Course SKU",
          Type = PropertyType.String,
          IsKey = false,
          IsCreateCounter = false,
          IsUpdateCounter = false,
          TypeAtSource = "string",
          IsNullable = false
        };

        schema.Properties.Add(property);
      }

      return schema;
    }

    /// <summary>
    /// Custom schema for Classes - Write
    /// </summary>
    /// <param name="schema"></param>
    /// <returns></returns>
    private static Schema GetClassesWrite(Schema schema)
    {
      var properties = new List<Property>
            {
                new Property
                {
                    Id = "open_seats",
                    Name = "open_seats",
                    Type = PropertyType.Integer,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "int",
                    IsNullable = true
                },
                new Property
                {
                    Id = "language",
                    Name = "language",
                    Type = PropertyType.String,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "string",
                    IsNullable = true
                },
                // new Property
                // {
                //     Id = "location_name",
                //     Name = "location_name",
                //     Type = PropertyType.String,
                //     IsKey = false,
                //     IsCreateCounter = false,
                //     IsUpdateCounter = false,
                //     TypeAtSource = "string",
                //     IsNullable = true
                // },
                new Property
                {
                    Id = "affiliation",
                    Name = "affiliation",
                    Type = PropertyType.String,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "string",
                    IsNullable = true
                },
                new Property
                {
                    Id = "location_city",
                    Name = "location_city",
                    Type = PropertyType.String,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "string",
                    IsNullable = true
                },
                new Property
                {
                    Id = "location_state",
                    Name = "location_state",
                    Type = PropertyType.String,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "string",
                    IsNullable = true
                },
                new Property
                {
                    Id = "location_state_province_county",
                    Name = "location_state_province_county",
                    Type = PropertyType.String,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "string",
                    IsNullable = true
                },
                new Property
                {
                    Id = "start_date",
                    Name = "start_date",
                    Type = PropertyType.Datetime,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "date",
                    IsNullable = true
                },
                new Property
                {
                    Id = "end_date",
                    Name = "end_date",
                    Type = PropertyType.Datetime,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "date",
                    IsNullable = true
                },
                new Property
                {
                    Id = "sku",
                    Name = "sku",
                    Type = PropertyType.String,
                    IsKey = true,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "string",
                    IsNullable = false
                },
                new Property
                {
                    Id = "course_sku",
                    Name = "course_sku",
                    Type = PropertyType.String,
                    IsKey = true,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "string",
                    IsNullable = false
                },
                new Property
                {
                    Id = "price",
                    Name = "price",
                    Type = PropertyType.String,
                    IsKey = true,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "string",
                    IsNullable = false
                },
                new Property
                {
                    Id = "visible",
                    Name = "visible",
                    Type = PropertyType.Bool,
                    IsKey = true,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "boolean",
                    IsNullable = false
                },
                new Property
                {
                    Id = "currency",
                    Name = "currency",
                    Type = PropertyType.String,
                    IsKey = true,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "string",
                    IsNullable = false
                },
                new Property
                {
                    Id = "startdatum",
                    Name = "startdatum",
                    Type = PropertyType.Datetime,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "date",
                    IsNullable = true
                },
                new Property
                {
                    Id = "enddatum",
                    Name = "enddatum",
                    Type = PropertyType.Datetime,
                    IsKey = false,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "date",
                    IsNullable = true
                },
                new Property
                {
                  Id = "external",
                  Name = "external",
                  Type = PropertyType.String,
                  IsKey = false,
                  IsCreateCounter = false,
                  IsUpdateCounter = false,
                  TypeAtSource = "string",
                  IsNullable = true
                },
            };

      schema.Properties.AddRange(properties);

      return schema;
    }
  }
}