using System;
using PluginRJGWebsite.Helper;
using Xunit;

namespace PluginRJGWebsiteTest.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateTest()
        {
            // setup
            var settings = new Settings{ Environment = "Production", Username = "test", Password = "test"};
            
            // act
            settings.Validate();

            // assert
        }
        
        [Fact]
        public void ValidateNullEnvironmentTest()
        {
            // setup
            var settings = new Settings{ Environment = null, Username = "test", Password = "test"};
            
            // act
            Exception e  = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("the Environment property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNullUsernameTest()
        {
            // setup
            var settings = new Settings{ Environment = "Production", Username = null, Password = "test"};
            
            // act
            Exception e  = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("the Username property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNullPasswordTest()
        {
            // setup
            var settings = new Settings{ Environment = "Production", Username = "test", Password = null};
            
            // act
            Exception e  = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("the Password property must be set", e.Message);
        }
    }
}