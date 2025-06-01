// src/Mapping/Attributes/UdtAttribute.cs
using System;

namespace CassandraDriver.Mapping.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class UdtAttribute : Attribute
    {
        public string UdtName { get; }

        public UdtAttribute(string udtName)
        {
            if (string.IsNullOrWhiteSpace(udtName))
            {
                throw new ArgumentNullException(nameof(udtName));
            }
            UdtName = udtName;
        }
    }
}
