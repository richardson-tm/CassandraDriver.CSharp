using System;

namespace CassandraDriver.Initializers
{
    public class CassandraStartupInitializerOptions
    {
        public bool Enabled { get; set; } = true;
        public string ValidationQuery { get; set; } = "SELECT release_version FROM system.local";
        public bool ThrowOnFailure { get; set; } = true;
        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
    }
}
