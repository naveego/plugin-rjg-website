using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Newtonsoft.Json;
using PluginRJGWebsite.DataContracts;
using PluginRJGWebsite.Helper;
using Pub;
using Field = PluginRJGWebsite.DataContracts.Field;

namespace PluginRJGWebsite.Plugin
{
    public class Plugin : Publisher.PublisherBase
    {
        private RequestHelper _client;
        private readonly HttpClient _injectedClient;
        private readonly ServerStatus _server;
        private readonly EndpointHelper _endpointHelper;
        private TaskCompletionSource<bool> _tcs;

        public Plugin(HttpClient client = null)
        {
            _injectedClient = client ?? new HttpClient();
            _server = new ServerStatus
            {
                Connected = false,
                WriteConfigured = false
            };
            _endpointHelper = new EndpointHelper();
        }

        /// <summary>
        /// Establishes a connection with RJG Website. Creates an authenticated http client and tests it.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>A message indicating connection success</returns>
        public override async Task<ConnectResponse> Connect(ConnectRequest request, ServerCallContext context)
        {
            _server.Connected = false;

            Logger.Info("Connecting...");

            // validate settings passed in
            try
            {
                _server.Settings = JsonConvert.DeserializeObject<Settings>(request.SettingsJson);
                _server.Settings.Validate();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return new ConnectResponse
                {
                    OauthStateJson = request.OauthStateJson,
                    ConnectionError = "",
                    OauthError = "",
                    SettingsError = e.Message
                };
            }

            // create new authenticated request helper with validated settings
            try
            {
                _client = new RequestHelper(_server.Settings, _injectedClient);
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }

            // attempt to call the RJG Website api
            try
            {
                var response = await _client.GetAsync("/rjg/v1/courses");
                response.EnsureSuccessStatusCode();

                _server.Connected = true;
                Logger.Info("Connected to RJG Website");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);

                return new ConnectResponse
                {
                    OauthStateJson = request.OauthStateJson,
                    ConnectionError = e.Message,
                    OauthError = "",
                    SettingsError = ""
                };
            }

            return new ConnectResponse
            {
                OauthStateJson = request.OauthStateJson,
                ConnectionError = "",
                OauthError = "",
                SettingsError = ""
            };
        }

        public override async Task ConnectSession(ConnectRequest request,
            IServerStreamWriter<ConnectResponse> responseStream, ServerCallContext context)
        {
            Logger.Info("Connecting session...");

            // create task to wait for disconnect to be called
            _tcs?.SetResult(true);
            _tcs = new TaskCompletionSource<bool>();

            // call connect method
            var response = await Connect(request, context);

            await responseStream.WriteAsync(response);

            Logger.Info("Session connected.");

            // wait for disconnect to be called
            await _tcs.Task;
        }


        /// <summary>
        /// Discovers schemas located in the RJG Website
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>Discovered schemas</returns>
        public override async Task<DiscoverSchemasResponse> DiscoverSchemas(DiscoverSchemasRequest request,
            ServerCallContext context)
        {
            Logger.Info("Discovering Schemas...");

            var discoverSchemasResponse = new DiscoverSchemasResponse();

            // only return requested schemas if refresh mode selected
            if (request.Mode == DiscoverSchemasRequest.Types.Mode.Refresh)
            {
                try
                {
                    var refreshSchemas = request.ToRefresh;

                    Logger.Info($"Refresh schemas attempted: {refreshSchemas.Count}");

                    var tasks = refreshSchemas.Select((s) =>
                        {
                            var endpoint = _endpointHelper.GetEndpointForName(s.Id);
                            return GetSchemaForEndpoint(endpoint);
                        })
                        .ToArray();

                    await Task.WhenAll(tasks);

                    discoverSchemasResponse.Schemas.AddRange(tasks.Where(x => x.Result != null).Select(x => x.Result));

                    // return refresh schemas 
                    Logger.Info($"Schemas returned: {discoverSchemasResponse.Schemas.Count}");
                    return discoverSchemasResponse;
                }
                catch (Exception e)
                {
                    Logger.Error(e.Message);
                    throw;
                }
            }

            // get all schemas
            try
            {
                Logger.Info($"Schemas attempted: {_endpointHelper.Endpoints.Count}");

                var tasks = _endpointHelper.Endpoints.Select(GetSchemaForEndpoint)
                    .ToArray();

                await Task.WhenAll(tasks);

                discoverSchemasResponse.Schemas.AddRange(tasks.Where(x => x.Result != null).Select(x => x.Result));
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }

            // return all schemas otherwise
            Logger.Info($"Schemas returned: {discoverSchemasResponse.Schemas.Count}");
            return discoverSchemasResponse;
        }

        /// <summary>
        /// Publishes a stream of data for a given schema
        /// </summary>
        /// <param name="request"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task ReadStream(ReadRequest request, IServerStreamWriter<Record> responseStream,
            ServerCallContext context)
        {
            var schema = request.Schema;
            var limit = request.Limit;
            var limitFlag = request.Limit != 0;
            var endpoint = _endpointHelper.GetEndpointForName(schema.Id);

            Logger.Info($"Publishing records for schema: {schema.Name}");

            try
            {
                var recordsCount = 0;
                var records = new List<Dictionary<string, object>>();

                // get all records
                if (!String.IsNullOrEmpty(endpoint.MetaDataPath))
                {
                    var tasks = endpoint.ReadPaths.Select(GetRecordsHasMetaDataPath)
                        .ToArray();

                    await Task.WhenAll(tasks);

                    records.AddRange(tasks.SelectMany(x => x.Result).ToList());
                }
                else
                {
                    var tasks = endpoint.ReadPaths.Select(GetRecordsNoMetaDataPath)
                        .ToArray();

                    await Task.WhenAll(tasks);

                    records.AddRange(tasks.SelectMany(x => x.Result).ToList());
                }

                foreach (var record in records)
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
                                        Data = value
                                    };
                                    break;
                            }
                        }
                    }

                    var recordOutput = new Record
                    {
                        Action = Record.Types.Action.Upsert,
                        DataJson = JsonConvert.SerializeObject(record)
                    };


                    // stop publishing if the limit flag is enabled and the limit has been reached or the server is disconnected
                    if ((limitFlag && recordsCount == limit) || !_server.Connected)
                    {
                        break;
                    }

                    // publish record
                    await responseStream.WriteAsync(recordOutput);
                    recordsCount++;
                }

                Logger.Info($"Published {recordsCount} records");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }

        /// <summary>
        /// Prepares the plugin to handle a write request
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<PrepareWriteResponse> PrepareWrite(PrepareWriteRequest request, ServerCallContext context)
        {
            Logger.Info("Preparing write...");
            _server.WriteConfigured = false;

            var writeSettings = new WriteSettings
            {
                CommitSLA = request.CommitSlaSeconds,
                Schema = request.Schema
            };

            _server.WriteSettings = writeSettings;
            _server.WriteConfigured = true;

            Logger.Info("Write prepared.");
            return Task.FromResult(new PrepareWriteResponse());
        }

        /// <summary>
        /// Takes in records and writes them out to the RJG Website then sends acks back to the client
        /// </summary>
        /// <param name="requestStream"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task WriteStream(IAsyncStreamReader<Record> requestStream,
            IServerStreamWriter<RecordAck> responseStream, ServerCallContext context)
        {
            try
            {
                Logger.Info("Writing records to RJG Website...");
                var schema = _server.WriteSettings.Schema;
                var sla = _server.WriteSettings.CommitSLA;
                var inCount = 0;
                var outCount = 0;

                // get next record to publish while connected and configured
                while (await requestStream.MoveNext(context.CancellationToken) && _server.Connected &&
                       _server.WriteConfigured)
                {
                    var record = requestStream.Current;
                    inCount++;

                    Logger.Debug($"Got record: {record.DataJson}");

                    // send record to source system
                    // timeout if it takes longer than the sla
                    Task<string> task;

                    if (record.Action == Record.Types.Action.Delete)
                    {
                        task = Task.Run(() => DeleteRecord(schema, record));
                    }
                    else
                    {
                        task = Task.Run(() => PutRecord(schema, record));
                    }
                    if (task.Wait(TimeSpan.FromSeconds(sla)))
                    {
                        // send ack
                        var ack = new RecordAck
                        {
                            CorrelationId = record.CorrelationId,
                            Error = task.Result
                        };
                        await responseStream.WriteAsync(ack);

                        if (String.IsNullOrEmpty(task.Result))
                        {
                            outCount++;
                        }
                    }
                    else
                    {
                        // send timeout ack
                        var ack = new RecordAck
                        {
                            CorrelationId = record.CorrelationId,
                            Error = "timed out"
                        };
                        await responseStream.WriteAsync(ack);
                    }
                }

                Logger.Info($"Wrote {outCount} of {inCount} records to RJG Website.");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }

        /// <summary>
        /// Handles disconnect requests from the agent
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<DisconnectResponse> Disconnect(DisconnectRequest request, ServerCallContext context)
        {
            // clear connection
            _server.Connected = false;
            _server.Settings = null;

            // alert connection session to close
            if (_tcs != null)
            {
                _tcs.SetResult(true);
                _tcs = null;
            }

            Logger.Info("Disconnected");
            return Task.FromResult(new DisconnectResponse());
        }

        /// <summary>
        /// Gets a schema for a given endpoint
        /// </summary>
        /// <param name="endpoint"></param>
        /// <returns>returns a schema or null if unavailable</returns>
        private async Task<Schema> GetSchemaForEndpoint(Endpoint endpoint)
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

            try
            {
                Logger.Debug($"Getting fields for: {endpoint.Name}");

                // get fields for endpoint
                if (!String.IsNullOrEmpty(endpoint.MetaDataPath))
                {
                    var response = await _client.GetAsync(endpoint.MetaDataPath);

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

                        if (field.FieldKey.Split("_").Length > 1)
                        {
                            var property = new Property
                            {
                                Id = field.FieldKey.Split("_")[1],
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
                    var response = await _client.GetAsync(endpoint.ReadPaths.First());

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

                Logger.Debug($"Added schema for: {endpoint.Name}");
                return schema;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return null;
            }
        }

        /// <summary>
        /// Gets the Naveego type from the provided RJG information
        /// </summary>
        /// <param name="field"></param>
        /// <returns>The property type</returns>
        private PropertyType GetPropertyTypeFromField(Field field)
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
        private Dictionary<string,PropertyType> GetPropertyTypesFromRecords(List<Dictionary<string, object>> records)
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
                                {PropertyType.Bool, 0},
                                {PropertyType.Integer, 0},
                                {PropertyType.Json, 0},
                                {PropertyType.Datetime, 0},
                                {PropertyType.String, 0}
                            });
                        }
                        
                        var value = record[recordKey];
                        var type = value != null
                            ? value.GetType().ToString().ToLower()
                            : "null";

                        if (type.Contains("null"))
                        {
                            continue;
                        }
                        
                        if (type.Contains("boolean"))
                        {
                            discoveredTypes[recordKey][PropertyType.Bool]++;
                        }
                        else if (type.Contains("int"))
                        {
                            discoveredTypes[recordKey][PropertyType.Integer]++;
                        }
                        else if (type.Contains("json"))
                        {
                            discoveredTypes[recordKey][PropertyType.Json]++;
                        }
                        else
                        {
                            if (DateTime.TryParse(value.ToString(), out DateTime d))
                            {
                                discoveredTypes[recordKey][PropertyType.Datetime]++;
                            }
                            else
                            {
                                discoveredTypes[recordKey][PropertyType.String]++;
                            }
                        }
                    }
                }
                
                // return object
                var outTypes = new Dictionary<string,PropertyType>();
                
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
                Logger.Info(e.Message);
                throw;
            }
        }

        /// <summary>
        /// Gets all records and prepares them to be read
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        private async Task<List<Dictionary<string, object>>> GetRecordsHasMetaDataPath(string path)
        {
            var records = new List<Dictionary<string, object>>();
            try
            {
                var response = await _client.GetAsync(path);
                response.EnsureSuccessStatusCode();

                var recordsResponse =
                    JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, object>>>(
                        await response.Content.ReadAsStringAsync());

                foreach (var record in recordsResponse)
                {
                    var outData = new Dictionary<string,object>();
                    var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(
                        JsonConvert.SerializeObject(record.Value["meta"]));
                    
                    outData.Add("id", record.Value["id"]);

                    foreach (var field in data)
                    {
                        outData.Add(field.Key.Split("_")[1], field.Value);
                    }
                    
                    records.Add(outData);
                }

                return records;
            }
            catch
            {
                Logger.Info($"No records for path {path}");
                return records;
            }
        }

        /// <summary>
        /// Gets all records
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        private async Task<List<Dictionary<string, object>>> GetRecordsNoMetaDataPath(string path)
        {
            var records = new List<Dictionary<string, object>>();
            try
            {
                var response = await _client.GetAsync(path);
                response.EnsureSuccessStatusCode();

                var recordsResponse =
                    JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(
                        await response.Content.ReadAsStringAsync());

                foreach (var record in recordsResponse)
                {
                    records.Add(record);
                }

                return records;
            }
            catch
            {
                Logger.Info($"No records for path {path}");
                return records;
            }
        }

        /// <summary>
        /// Writes a record out to RJG Website
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="record"></param>
        /// <returns></returns>
        private async Task<string> PutRecord(Schema schema, Record record)
        {
            Dictionary<string, object> recObj;
            var endpoint = _endpointHelper.GetEndpointForName(schema.Id);

            if (String.IsNullOrEmpty(endpoint.MetaDataPath))
            {
                try
                {
                    // check if source has newer record than write back record
                    recObj = JsonConvert.DeserializeObject<Dictionary<string, object>>(record.DataJson);

                    if (recObj.ContainsKey("id"))
                    {
                        if (recObj["id"] != null)
                        {
                            // record already exists, check date then patch it
                            var id = recObj["id"];

                            // build and send request
                            var path = String.Format("{0}/{1}", endpoint.ReadPaths.First(), id);

                            var response = await _client.GetAsync(path);
                            response.EnsureSuccessStatusCode();

                            var srcObj =
                                JsonConvert.DeserializeObject<Dictionary<string, object>>(
                                    await response.Content.ReadAsStringAsync());

                            // get modified key from schema
                            var modifiedKey = schema.Properties.First(x => x.IsUpdateCounter);

                            if (recObj.ContainsKey(modifiedKey.Id) && srcObj.ContainsKey(modifiedKey.Id))
                            {
                                if (recObj[modifiedKey.Id] != null && srcObj[modifiedKey.Id] != null)
                                {
                                    // if source is newer than request exit
                                    if (DateTime.Parse(recObj[modifiedKey.Id].ToString()) <=
                                        DateTime.Parse(srcObj[modifiedKey.Id].ToString()))
                                    {
                                        Logger.Info($"Source is newer for record {record.DataJson}");
                                        return "source system is newer than requested write back";
                                    }
                                }
                            }

                            var patchObj = GetPatchObject(endpoint, recObj);

                            var content = new StringContent(JsonConvert.SerializeObject(patchObj), Encoding.UTF8,
                                "application/json");

                            response = await _client.PatchAsync(path, content);
                            response.EnsureSuccessStatusCode();

                            Logger.Info("Modified 1 record.");
                            return "";
                        }
                        else
                        {
                            // record does not exist, create it
                            var postObj = GetPostObject(endpoint, recObj);

                            var content = new StringContent(JsonConvert.SerializeObject(postObj), Encoding.UTF8,
                                "application/json");

                            var response = await _client.PostAsync(endpoint.ReadPaths.First(), content);
                            response.EnsureSuccessStatusCode();

                            Logger.Info("Created 1 record.");
                            return "";
                        }
                    }

                    return "Key 'id' not found on requested record to write back.";
                }
                catch (Exception e)
                {
                    Logger.Error(e.Message);
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
        private object GetPatchObject(Endpoint endpoint, Dictionary<string, object> recObj)
        { 
            switch (endpoint.Name)
            {
                case "Classes":
                    return new ClassesPatchObject
                    {
                        OpenSeats = String.IsNullOrEmpty(recObj["open_seats"].ToString()) ? int.Parse(recObj["open_seats"].ToString()) : 0,
                        Language = recObj["language"].ToString(),
                        Location = recObj["location"].ToString(),
                        StartDate = recObj["start_date"].ToString(),
                        EndDate = recObj["end_date"].ToString(),
                        CourseSKU = recObj["course_sku"].ToString(),
                        Price = recObj["price"].ToString()
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
        private object GetPostObject(Endpoint endpoint, Dictionary<string, object> recObj)
        {
            switch (endpoint.Name)
            {
                case "Classes":
                    return new ClassesPostObject
                    {
                        OpenSeats = String.IsNullOrEmpty(recObj["open_seats"].ToString()) ? int.Parse(recObj["open_seats"].ToString()) : 0,
                        Language = recObj["language"].ToString(),
                        Location = recObj["location"].ToString(),
                        StartDate = recObj["start_date"].ToString(),
                        EndDate = recObj["end_date"].ToString(),
                        SKU = recObj["sku"].ToString(),
                        CourseSKU = recObj["course_sku"].ToString(),
                        Price = recObj["price"].ToString()
                    };
                default:
                    return new object();
            }
        }

        /// <summary>
        /// Deletes a record from the RJG Website
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="record"></param>
        /// <returns></returns>
        private async Task<string> DeleteRecord(Schema schema, Record record)
        {
            Dictionary<string, object> recObj;
            var endpoint = _endpointHelper.GetEndpointForName(schema.Id);

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
                                    var response = await _client.DeleteAsync(uri);
                                    response.EnsureSuccessStatusCode();

                                    Logger.Info("Deleted 1 record.");
                                    return "";
                                }
                                catch (Exception e)
                                {
                                    Logger.Error(e.Message);
                                }
                            }    
                        }
                        
                        return "Could not delete record with no id.";
                    }

                    return "Key 'id' not found on requested record to write back.";
                }
                catch (Exception e)
                {
                    Logger.Error(e.Message);
                    return e.Message;
                }
            }

            // code for modifying forms would go here if needed but currently is not needed

            return "Write backs are only supported for Classes.";
        }
    }
}