using System.Linq;
using EventStore.Core.Index;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    
    public class table_index_on_try_get_one_value_query: IUseFixture<table_index_on_try_get_one_value_query.FixtureData>
    {
        private TableIndex _tableIndex;
        private string _indexDir;

        public class FixtureData : SpecificationWithDirectoryPerTestFixture
        {
            public TableIndex _tableIndex;
            public string _indexDir;

            public FixtureData()
            {
                _indexDir = PathName;
                var fakeReader = new TFReaderLease(new FakeTfReader());
                _tableIndex = new TableIndex(_indexDir,
                                             () => new HashListMemTable(maxSize: 10),
                                             () => fakeReader,
                                             maxSizeForMemory: 5);
                _tableIndex.Initialize(long.MaxValue);

                _tableIndex.Add(0, 0xDEAD, 0, 0xFF00);
                _tableIndex.Add(0, 0xDEAD, 1, 0xFF01);

                _tableIndex.Add(0, 0xBEEF, 0, 0xFF00);
                _tableIndex.Add(0, 0xBEEF, 1, 0xFF01);

                _tableIndex.Add(0, 0xABBA, 0, 0xFF00); // 1st ptable0

                _tableIndex.Add(0, 0xABBA, 1, 0xFF01);
                _tableIndex.Add(0, 0xABBA, 2, 0xFF02);
                _tableIndex.Add(0, 0xABBA, 3, 0xFF03);

                _tableIndex.Add(0, 0xADA, 0, 0xFF00); // simulates duplicate due to concurrency in TableIndex (see memtable below)
                _tableIndex.Add(0, 0xDEAD, 0, 0xFF10); // 2nd ptable0

                _tableIndex.Add(0, 0xDEAD, 1, 0xFF11); // in memtable
                _tableIndex.Add(0, 0xADA, 0, 0xFF00); // in memtable                
            }

            public override void Dispose()
            {
                _tableIndex.Close();

                base.Dispose();
            }
        }
        
        public void SetFixture(FixtureData data)
        {
            _indexDir = data._indexDir;
            _tableIndex = data._tableIndex;
        }

        [Fact]
        public void should_return_empty_collection_when_stream_is_not_in_db()
        {
            long position;
            Assert.False(_tableIndex.TryGetOneValue(0xFEED, 0, out position));
        }

        [Fact]
        public void should_return_element_with_largest_position_when_hash_collisions()
        {
            long position;
            Assert.True(_tableIndex.TryGetOneValue(0xDEAD, 0, out position));
            Assert.Equal(0xFF10, position);
        }

        [Fact]
        public void should_return_only_one_element_if_concurrency_duplicate_happens_on_range_query_as_well()
        {
            var res = _tableIndex.GetRange(0xADA, 0, 100).ToList();
            Assert.Equal(1, res.Count());
            Assert.Equal(0xADAu, res[0].Stream);
            Assert.Equal(0, res[0].Version);
            Assert.Equal(0xFF00, res[0].Position);
        }
    }
}