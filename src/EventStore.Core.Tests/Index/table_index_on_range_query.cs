using System;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class table_index_on_range_query  : IClassFixture<table_index_on_range_query.Fixture>
    {
        private TableIndex _tableIndex;

        public class Fixture : SpecificationWithDirectoryPerTestFixture
        {
            public readonly TableIndex _tableIndex;

            public Fixture()
            {
                _tableIndex = new TableIndex(PathName,
                                         () => new HashListMemTable(maxSize: 40),
                                         () => { throw new InvalidOperationException(); },
                                         maxSizeForMemory: 20);
            _tableIndex.Initialize(long.MaxValue);

            _tableIndex.Add(0, 0xDEAD, 0, 0xFF00);
            _tableIndex.Add(0, 0xDEAD, 1, 0xFF01); 
                            
            _tableIndex.Add(0, 0xBEEF, 0, 0xFF00);
            _tableIndex.Add(0, 0xBEEF, 1, 0xFF01); 
                             
            _tableIndex.Add(0, 0xABBA, 0, 0xFF00);
            _tableIndex.Add(0, 0xABBA, 1, 0xFF01); 
            _tableIndex.Add(0, 0xABBA, 2, 0xFF02);
            _tableIndex.Add(0, 0xABBA, 3, 0xFF03); 
                             
            _tableIndex.Add(0, 0xDEAD, 0, 0xFF10);
            _tableIndex.Add(0, 0xDEAD, 1, 0xFF11); 
                             
            _tableIndex.Add(0, 0xADA, 0, 0xFF00);
            }

            public override void Dispose()
            {
                _tableIndex.Close();
                base.Dispose();
            }
        }

        public table_index_on_range_query(Fixture data)
        {
            _tableIndex = data._tableIndex;
        }


        [Fact]
        public void should_return_empty_collection_when_stream_is_not_in_db()
        {
            var res = _tableIndex.GetRange(0xFEED, 0, 100);
            Assert.Empty(res);
        }

        [Fact]
        public void should_return_all_applicable_elements_in_correct_order()
        {
            var res = _tableIndex.GetRange(0xBEEF, 0, 100).ToList();
            Assert.Equal(2, res.Count());
            Assert.Equal(0xBEEFu, res[0].Stream);
            Assert.Equal(1,res[0].Version);
            Assert.Equal(0xFF01, res[0].Position);
            Assert.Equal(0xBEEFu,res[1].Stream);
            Assert.Equal(0,res[1].Version);
            Assert.Equal(0xFF00,res[1].Position);
        }

        [Fact]
        public void should_return_all_elements_with_hash_collisions_in_correct_order()
        {
            var res = _tableIndex.GetRange(0xDEAD, 0, 100).ToList();
            Assert.Equal(4, res.Count());
            Assert.Equal(0xDEADu, res[0].Stream);
            Assert.Equal(1, res[0].Version);
            Assert.Equal(0xFF11, res[0].Position);
                   
            Assert.Equal(0xDEADu, res[1].Stream);
            Assert.Equal(1, res[1].Version);
            Assert.Equal(0xFF01u, res[1].Position);
                   
            Assert.Equal(0xDEADu, res[2].Stream);
            Assert.Equal(0, res[2].Version);
            Assert.Equal(0xFF10u,res[2].Position);
                   
            Assert.Equal(0xDEADu,res[3].Stream);
            Assert.Equal(0, res[3].Version);
            Assert.Equal(0xFF00u, res[3].Position);
        }
    }
}
