using Cassandra; // For RowSet

namespace CassandraDriver.Results
{
    public class LwtResult<T> where T : class
    {
        /// <summary>
        /// Indicates whether the Lightweight Transaction (LWT) was applied.
        /// </summary>
        public bool Applied { get; }

        /// <summary>
        /// For non-applied LWTs (e.g., UPDATE ... IF condition), this may contain
        /// the current state of the entity based on the columns returned by Cassandra.
        /// For applied inserts, this might be null or the inserted entity.
        /// </summary>
        public T? Entity { get; }

        /// <summary>
        /// The raw RowSet returned by the Cassandra driver.
        /// Useful for accessing other LWT result columns if Cassandra returns more than just [applied].
        /// </summary>
        public RowSet RawRowSet { get; }

        public LwtResult(bool applied, RowSet rowSet, T? entity = null)
        {
            Applied = applied;
            RawRowSet = rowSet;
            Entity = entity;
        }
    }
}
