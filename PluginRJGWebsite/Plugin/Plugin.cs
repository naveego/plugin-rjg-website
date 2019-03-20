using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using Google.Protobuf.WellKnownTypes;
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
        private TaskCompletionSource<bool> _tcs;

        public Plugin(HttpClient client = null)
        {
            _injectedClient = client ?? new HttpClient();
            _server = new ServerStatus
            {
                Connected = false,
                WriteConfigured = false
            };
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

                var content = JsonConvert.DeserializeObject<List<object>>(await response.Content.ReadAsStringAsync());

                if (content.Count > 0)
                {
                    _server.Connected = true;

                    Logger.Info("Connected to RJG Website");
                }
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

            // endpoint helper contains all target endpoints
            var endPointHelper = new EndpointHelper();

            // only return requested schemas if refresh mode selected
            if (request.Mode == DiscoverSchemasRequest.Types.Mode.Refresh)
            {
                try
                {
                    var refreshSchemas = request.ToRefresh;

                    Logger.Info($"Refresh schemas attempted: {refreshSchemas.Count}");

                    var tasks = refreshSchemas.Select((s) =>
                        {
                            var endpoint = endPointHelper.GetEndpointForName(s.Id);
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
                Logger.Info($"Schemas attempted: {endPointHelper.Endpoints.Count}");

                var tasks = endPointHelper.Endpoints.Select(GetSchemaForEndpoint)
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
            var endpointHelper = new EndpointHelper();
            var endpoint = endpointHelper.GetEndpointForName(schema.Id);

            Logger.Info($"Publishing records for schema: {schema.Name}");

            try
            {
                var recordsCount = 0;
                var records = new List<Dictionary<string, object>>();

                // get all records
                if (!String.IsNullOrEmpty(endpoint.MetaDataPath))
                {
                    var tasks = endpoint.ReadPaths.Select(GetRecordsForMetaDataPath)
                        .ToArray();

                    await Task.WhenAll(tasks);

                    records.AddRange(tasks.SelectMany(x => x.Result).ToList());
                }
                else
                {
                    var tasks = endpoint.ReadPaths.Select(GetRecordsForNoMetaDataPath)
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
        /// Takes in records and writes them out to the Zoho instance then sends acks back to the client
        /// </summary>
        /// <param name="requestStream"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
//        public override async Task WriteStream(IAsyncStreamReader<Record> requestStream,
//            IServerStreamWriter<RecordAck> responseStream, ServerCallContext context)
//        {
//            try
//            {
//                Logger.Info("Writing records to Zoho...");
//                var schema = _server.WriteSettings.Schema;
//                var sla = _server.WriteSettings.CommitSLA;
//                var inCount = 0;
//                var outCount = 0;
//                
//                // get next record to publish while connected and configured
//                while (await requestStream.MoveNext(context.CancellationToken) && _server.Connected && _server.WriteConfigured)
//                {
//                    var record = requestStream.Current;
//                    inCount++;
//                    
//                    Logger.Debug($"Got record: {record.DataJson}");
//                    
//                    // send record to source system
//                    // timeout if it takes longer than the sla
//                    var task = Task.Run(() => PutRecord(schema,record));
//                    if (task.Wait(TimeSpan.FromSeconds(sla)))
//                    {
//                        // send ack
//                        var ack = new RecordAck
//                        {
//                            CorrelationId = record.CorrelationId,
//                            Error = task.Result
//                        };
//                        await responseStream.WriteAsync(ack);
//                        
//                        if (String.IsNullOrEmpty(task.Result))
//                        {
//                            outCount++;
//                        }
//                    }
//                    else
//                    {
//                        // send timeout ack
//                        var ack = new RecordAck
//                        {
//                            CorrelationId = record.CorrelationId,
//                            Error = "timed out"
//                        };
//                        await responseStream.WriteAsync(ack);
//                    }
//                }
//                
//                Logger.Info($"Wrote {outCount} of {inCount} records to Zoho.");
//            }
//            catch (Exception e)
//            {
//                Logger.Error(e.Message);
//                throw;
//            }
//        }

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

                    foreach (var fieldKey in fields.Keys)
                    {
                        var field = fields[fieldKey];

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
                else
                {
                    var response = await _client.GetAsync(endpoint.ReadPaths.First());

                    var recordsList =
                        JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(
                            await response.Content.ReadAsStringAsync());

                    var record = recordsList.First();

                    foreach (var recordKey in record.Keys)
                    {
                        var value = record[recordKey];
                        var property = new Property
                        {
                            Id = recordKey,
                            Name = recordKey,
                            Type = GetPropertyTypeFromValue(value),
                            IsKey = false,
                            IsCreateCounter = false,
                            IsUpdateCounter = false,
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
        /// <param name="value"></param>
        /// <returns>The property type</returns>
        private PropertyType GetPropertyTypeFromValue(object value)
        {
            try
            {
                // try datetime
                if (DateTime.TryParse(value.ToString(), out DateTime d))
                {
                    return PropertyType.Date;
                }

                // try int
                if (Int32.TryParse(value.ToString(), out int i))
                {
                    return PropertyType.Integer;
                }

                // try float
                if (float.TryParse(value.ToString(), out float f))
                {
                    return PropertyType.Float;
                }

                // try boolean
                if (bool.TryParse(value.ToString(), out bool b))
                {
                    return PropertyType.Bool;
                }

                // try string
                if (value is string)
                {
                    return PropertyType.String;
                }
                
                // try object or array
                if (value is IEnumerable)
                {
                    return PropertyType.Json;
                }

                return PropertyType.String;
            }
            catch (Exception e)
            {
                return PropertyType.String;
            }
        }

        /// <summary>
        /// Gets all records and prepares them to be read
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        private async Task<List<Dictionary<string, object>>> GetRecordsForMetaDataPath(string path)
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
                    var data = JsonConvert.DeserializeObject<Dictionary<string,object>>(JsonConvert.SerializeObject(record.Value["meta"]));
                    data.Add("id",record.Value["id"]);
                    records.Add(data);
                }

                return records;
            }
            catch (Exception e)
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
        private async Task<List<Dictionary<string, object>>> GetRecordsForNoMetaDataPath(string path)
        {
            var records = new List<Dictionary<string, object>>();
            try
            {
                var response = await _client.GetAsync(path);
                response.EnsureSuccessStatusCode();

                var recordsResponse =
                    JsonConvert.DeserializeObject<List<Dictionary<string,object>>>(
                        await response.Content.ReadAsStringAsync());

                foreach (var record in recordsResponse)
                {
                    records.Add(record);
                }

                return records;
            }
            catch (Exception e)
            {
                Logger.Info($"No records for path {path}");
                return records;
            }
        }

        /// <summary>
        /// Writes a record out to Zoho
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="record"></param>
        /// <returns></returns>
//        private async Task<string> PutRecord(Schema schema, Record record)
//        {
//            Dictionary<string, object> recObj;
//            
//            // get information from schema
//            var moduleName = GetModuleName(schema);
//            
//            try
//            {
//                // check if source has newer record than write back record
//                recObj = JsonConvert.DeserializeObject<Dictionary<string, object>>(record.DataJson);
//
//                if (recObj.ContainsKey("id"))
//                {
//                    var id = recObj["id"];
//                
//                    // build and send request
//                    var uri = String.Format("https://www.zohoapis.com/crm/v2/{0}/{1}", moduleName, id ?? "null");
//
//                    var response = await _client.GetAsync(uri);
//                    if (IsSuccessAndNotEmpty(response))
//                    {
//                        var recordsResponse = JsonConvert.DeserializeObject<RecordsResponse>(await response.Content.ReadAsStringAsync());
//                        var srcObj = recordsResponse.data[0];
//                
//                        // get modified key from schema
//                        var modifiedKey = schema.Properties.First(x => x.IsUpdateCounter);
//
//                        if (recObj.ContainsKey(modifiedKey.Id) && srcObj.ContainsKey(modifiedKey.Id))
//                        {
//                            if (recObj[modifiedKey.Id] != null && srcObj[modifiedKey.Id] != null)
//                            {
//                                // if source is newer than request then exit
//                                if (DateTime.Parse((string) recObj[modifiedKey.Id]) <=
//                                    DateTime.Parse((string) srcObj[modifiedKey.Id]))
//                                {
//                                    Logger.Info($"Source is newer for record {record.DataJson}");
//                                    return "source system is newer than requested write back";
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//            catch (Exception e)
//            {
//                Logger.Error(e.Message);
//                return e.Message;
//            }
//            try
//            {   
//                // build and send request
//                var uri = String.Format("https://www.zohoapis.com/crm/v2/{0}/upsert", moduleName);
//                
//                var putRequestObj = new PutRequest
//                {
//                    data = new [] {recObj},
//                    trigger = new string[0]
//                };
//
//                var content = new StringContent(JsonConvert.SerializeObject(putRequestObj), Encoding.UTF8, "application/json");
//                
//                var response = await _client.PostAsync(uri, content);
//                
//                response.EnsureSuccessStatusCode();
//                
//                Logger.Info("Modified 1 record.");
//                return "";
//            }
//            catch (Exception e)
//            {
//                Logger.Error(e.Message);
//                return e.Message;
//            }
//        }

        /// <summary>
        /// Checks if a http response message is not empty and did not fail
        /// </summary>
        /// <param name="response"></param>
        /// <returns></returns>
        private bool IsSuccessAndNotEmpty(HttpResponseMessage response)
        {
            return response.StatusCode != HttpStatusCode.NoContent && response.IsSuccessStatusCode;
        }
    }
}