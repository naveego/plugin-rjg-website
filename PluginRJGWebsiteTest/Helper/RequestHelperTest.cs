using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using PluginRJGWebsite.Helper;
using RichardSzalay.MockHttp;
using Xunit;

namespace PluginRJGWebsiteTest.Helper
{
    public class RequestHelperTest
    {
        [Fact]
        public async Task GetAsyncTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();
            
            mockHttp.When("https://rjginc.com/wp-json/jwt-auth/v1/token")
                .Respond("application/json", "{\"token\":\"mocktoken\"}");

            mockHttp.When("https://rjginc.com/wp-json/jwt-auth/v1/token/validate")
                .Respond(HttpStatusCode.OK);

            mockHttp.When("https://rjginc.com/wp-json/path")
                .Respond("application/json", "success");

            var requestHelper = new RequestHelper(new Settings{ Environment = "Production", Username = "test", Password = "test"}, mockHttp.ToHttpClient());
            
            // act
            var response = await requestHelper.GetAsync("/path");

            // assert
            Assert.Equal("success", await response.Content.ReadAsStringAsync());
        }
        
        [Fact]
        public async Task GetAsyncWithTokenExceptionTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();
            
            mockHttp.When("https://rjginc.com/wp-json/jwt-auth/v1/token")
                .Respond(HttpStatusCode.Forbidden);
            
            mockHttp.When("https://rjginc.com/wp-json/jwt-auth/v1/token/validate")
                .Respond(HttpStatusCode.Forbidden);

            mockHttp.When("https://rjginc.com/wp-json/path")
                .Respond(HttpStatusCode.Unauthorized);

            var requestHelper = new RequestHelper(new Settings{ Environment = "Production", Username = "test", Password = "test"}, mockHttp.ToHttpClient());
            
            // act
            Exception e  = await Assert.ThrowsAsync<HttpRequestException>(async () => await requestHelper.GetAsync("/path"));

            // assert
            Assert.Contains("403", e.Message);
        }
        
        [Fact]
        public async Task GetAsyncWithRequestExceptionTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();
           
            mockHttp.When("https://rjginc.com/wp-json/jwt-auth/v1/token")
                .Respond("application/json", "{\"token\":\"mocktoken\"}");

            mockHttp.When("https://rjginc.com/wp-json/jwt-auth/v1/token/validate")
                .Respond(HttpStatusCode.OK);

            mockHttp.When("https://rjginc.com/wp-json/path")
                .Throw(new Exception("bad stuff"));

            var requestHelper = new RequestHelper(new Settings{ Environment = "Production", Username = "test", Password = "test"}, mockHttp.ToHttpClient());
            
            // act
            Exception e  = await Assert.ThrowsAsync<Exception>(async () => await requestHelper.GetAsync("/path"));

            // assert
            Assert.Contains("bad stuff", e.Message);
        }
    }
}