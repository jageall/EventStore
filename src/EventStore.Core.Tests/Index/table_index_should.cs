using System;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class table_index_should : IUseFixture<table_index_should.FixtureData>
    {
        private TableIndex _tableIndex;
        public class FixtureData : SpecificationWithDirectoryPerTestFixture
        {
            public readonly TableIndex _tableIndex;

            public FixtureData()
            {
                _tableIndex = new TableIndex(PathName,
                    () => new HashListMemTable(maxSize: 20),
                    () => { throw new InvalidOperationException(); },
                    maxSizeForMemory: 10);
                _tableIndex.Initialize(long.MaxValue);
            }

            public override void Dispose()
            {
                _tableIndex.Close();
                base.Dispose();
            }
        }

        public void SetFixture(FixtureData data)
        {
            _tableIndex = data._tableIndex;
        }

        [Fact]
        public void throw_argumentoutofrangeexception_on_range_query_when_provided_with_negative_start_version()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => _tableIndex.GetRange(0x0000, -1, int.MaxValue).ToArray());
        }

        [Fact]
        public void throw_argumentoutofrangeexception_on_range_query_when_provided_with_negative_end_version()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => _tableIndex.GetRange(0x0000, 0, -1).ToArray());
        }

        [Fact]
        public void throw_argumentoutofrangeexception_on_get_one_entry_query_when_provided_with_negative_version()
        {
            long pos;
            Assert.Throws<ArgumentOutOfRangeException>(() => _tableIndex.TryGetOneValue(0x0000, -1, out pos));
        }

        [Fact]
        public void throw_argumentoutofrangeexception_on_adding_entry_with_negative_commit_position()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => _tableIndex.Add(-1, 0x0000, 0, 0));
        }

        [Fact]
        public void throw_argumentoutofrangeexception_on_adding_entry_with_negative_version()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => _tableIndex.Add(0, 0x0000, -1, 0));
        }

        [Fact]
        public void throw_argumentoutofrangeexception_on_adding_entry_with_negative_position()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => _tableIndex.Add(0, 0x0000, 0, -1));
        }
    }
}