using System;

namespace CassandraDriver.Mapping.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
    public class TableAttribute : Attribute
    {
        public string Name { get; }

        public TableAttribute(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }
            Name = name;
        }
    }
}
