using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public abstract class PTableReadScenario: SpecificationWithFile
    {
        private readonly int _midpointCacheDepth;

        protected PTable PTable;

        protected PTableReadScenario(int midpointCacheDepth)
        {
            _midpointCacheDepth = midpointCacheDepth;
            var table = new HashListMemTable(maxSize: 50);

            AddItemsForScenario(table);

            PTable = PTable.FromMemtable(table, Filename, cacheDepth: _midpointCacheDepth);
        }

        public override void Dispose()
        {
            PTable.Dispose();
            
            base.Dispose();
        }

        protected abstract void AddItemsForScenario(IMemTable memTable);
    }
}