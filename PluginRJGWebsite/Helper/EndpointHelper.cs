using System.Collections.Generic;
using System.Linq;
using Pub;

namespace PluginRJGWebsite.Helper
{
    // https://dev.clockwork360.com/rjg/wp-json/frm/v2/forms
    public class EndpointHelper
    {
        public List<Endpoint> Endpoints { get; set; }

        public EndpointHelper()
        {
            Endpoints = new List<Endpoint>
            {
                new Endpoint
                {
                    Name = "Courses",
                    MetaDataPath = "",
                    ReadPaths = new List<string>
                    {
                        "/rjg/v1/courses"
                    },
                    DataFlowDirection = Schema.Types.DataFlowDirection.Read
                },
                new Endpoint
                {
                    Name = "Classes - Read",
                    MetaDataPath = "",
                    ReadPaths = new List<string>
                    {
                        "/rjg/v1/classes"
                    },
                    DataFlowDirection = Schema.Types.DataFlowDirection.Read
                },
                new Endpoint
                {
                    Name = "Classes - Write",
                    MetaDataPath = "",
                    ReadPaths = new List<string>
                    {
                        "/rjg/v1/classes"
                    },
                    DataFlowDirection = Schema.Types.DataFlowDirection.Write
                },
                new Endpoint
                {
                    Name = "Registrations",
                    MetaDataPath = "",
                    ReadPaths = new List<string>
                    {
                        "/rjg/v1/registrations"
                    },
                    DataFlowDirection = Schema.Types.DataFlowDirection.Read
                },
                new Endpoint
                {
                    Name = "Contact Us",
                    MetaDataPath = "/frm/v2/forms/2/fields",
                    ReadPaths = new List<string>
                    {
                        "/frm/v2/forms/2/entries",
                        "/frm/v2/forms/33/entries",
                        "/frm/v2/forms/24/entries",
                        "/frm/v2/forms/34/entries",
                        "/frm/v2/forms/35/entries",
                        "/frm/v2/forms/15/entries"
                    },
                    DataFlowDirection = Schema.Types.DataFlowDirection.Read
                },
                new Endpoint
                {
                    Name = "General Sales Inquiries",
                    MetaDataPath = "/frm/v2/forms/11/fields",
                    ReadPaths = new List<string>
                    {
                        "/frm/v2/forms/11/entries",
                        "/frm/v2/forms/16/entries",
                        "/frm/v2/forms/20/entries"
                    },
                    DataFlowDirection = Schema.Types.DataFlowDirection.Read
                },
                new Endpoint
                {
                    Name = "Assessments",
                    MetaDataPath = "/frm/v2/forms/7/fields",
                    ReadPaths = new List<string>
                    {
                        "/frm/v2/forms/7/entries",
                        "/frm/v2/forms/23/entries",
                        "/frm/v2/forms/28/entries",
                        "/frm/v2/forms/27/entries",
                        "/frm/v2/forms/14/entries",
                        "/frm/v2/forms/19/entries"
                    },
                    DataFlowDirection = Schema.Types.DataFlowDirection.Read
                },
                new Endpoint
                {
                    Name = "Sensor Placements",
                    MetaDataPath = "/frm/v2/forms/37/fields",
                    ReadPaths = new List<string>
                    {
                        "/frm/v2/forms/37/entries"
                    },
                    DataFlowDirection = Schema.Types.DataFlowDirection.Read
                },
                new Endpoint
                {
                    Name = "Wait List Registrations",
                    MetaDataPath = "/frm/v2/forms/3/fields",
                    ReadPaths = new List<string>
                    {
                        "/frm/v2/forms/3/entries"
                    },
                    DataFlowDirection = Schema.Types.DataFlowDirection.Read
                }
            };
        }

        public Endpoint GetEndpointForName(string name)
        {
            return Endpoints.First(e => e.Name == name);
        }
    }

    public class Endpoint
    {
        public string Name { get; set; }
        public string MetaDataPath { get; set; }
        public List<string> ReadPaths { get; set; }
        public Schema.Types.DataFlowDirection DataFlowDirection { get; set; }
    }
}