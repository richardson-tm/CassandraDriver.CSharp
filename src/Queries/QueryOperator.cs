namespace CassandraDriver.Queries
{
    public enum QueryOperator
    {
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        In // Added IN operator for simple cases
        // Contains, StartsWith, EndsWith could be added later for text searching if supported by Cassandra extensions/SASI
    }
}
