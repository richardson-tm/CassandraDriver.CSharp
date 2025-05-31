using System;

namespace CassandraDriver.Mapping.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class ClusteringKeyAttribute : Attribute
    {
        public int Order { get; }

        public ClusteringKeyAttribute(int order = 0)
        {
            Order = order;
        }
    }
}
