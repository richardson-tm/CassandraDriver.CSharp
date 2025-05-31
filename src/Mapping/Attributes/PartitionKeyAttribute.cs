using System;

namespace CassandraDriver.Mapping.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class PartitionKeyAttribute : Attribute
    {
        public int Order { get; }

        public PartitionKeyAttribute(int order = 0)
        {
            Order = order;
        }
    }
}
