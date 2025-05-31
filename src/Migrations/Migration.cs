namespace CassandraDriver.Migrations
{
    public class Migration : IComparable<Migration>
    {
        public long Version { get; }
        public string Description { get; }
        public string ScriptName { get; }
        public string ScriptContent { get; }

        public Migration(long version, string description, string scriptName, string scriptContent)
        {
            Version = version;
            Description = description ?? throw new ArgumentNullException(nameof(description));
            ScriptName = scriptName ?? throw new ArgumentNullException(nameof(scriptName));
            ScriptContent = scriptContent ?? throw new ArgumentNullException(nameof(scriptContent));
        }

        public int CompareTo(Migration? other)
        {
            if (other == null) return 1;
            return Version.CompareTo(other.Version);
        }

        public override string ToString() => $"{Version:D3}: {Description} ({ScriptName})";
    }
}
