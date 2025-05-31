using System;

namespace CassandraDriver.Mapping.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class ColumnAttribute : Attribute
    {
        public string Name { get; }

        public ColumnAttribute(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }
            Name = name;
        }
    }
}
