using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using PluginRJGWebsite.Helper;
using RichardSzalay.MockHttp;
using Xunit;

namespace PluginRJGWebsiteTest.Helper
{
    public class AuthenticatorTest
    {
        [Fact]
        public async Task GetTokenTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When("https://rjginc.com/wp-json/jwt-auth/v1/token")
                .Respond("application/json", "{\"token\":\"mocktoken\"}");

            mockHttp.When("https://rjginc.com/wp-json/jwt-auth/v1/token/validate")
                .Respond(HttpStatusCode.OK);
            
            var auth = new Authenticator(new Settings{ Environment = "Production", Username = "test", Password = "test"}, mockHttp.ToHttpClient());
            
            // act
            var token = await auth.GetToken();
            var token2 = await auth.GetToken();

            // assert
            // first token is fetched
            Assert.Equal("mocktoken", token);
            // second token should be the same but not be fetched
            Assert.Equal("mocktoken", token2);
        }
        
        [Fact]
        public async Task GetTokenWithExceptionTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When("https://rjginc.com/wp-json/jwt-auth/v1/token")
                .Respond(HttpStatusCode.Forbidden);

            var auth = new Authenticator(new Settings{ Environment = "Production", Username = "test", Password = "test"}, mockHttp.ToHttpClient());
            
            // act
            Exception e  = await Assert.ThrowsAsync<HttpRequestException>(async () => await auth.GetToken());

            // assert
            Assert.Contains("403", e.Message);
        }
    }
}