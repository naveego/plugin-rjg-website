namespace PluginRJGWebsite.Helper
{
    public class EnvironmentHelper
    {
        public string Endpoint { get; }

        public EnvironmentHelper(string environment)
        {
            switch (environment)
            {
                case "Development":
                    Endpoint = "https://rjgdev.clockwork360.com/wp-json";
                    return;
                case "Production":
                    Endpoint = "https://rjginc.com/wp-json";
                    return;
                default:
                    Logger.Error($"Environment {environment} not known. Unable to get config.");
                    return; 
            }
        }
    }
}