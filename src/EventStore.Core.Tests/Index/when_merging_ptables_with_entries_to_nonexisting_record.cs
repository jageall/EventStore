using System.Collections.Generic;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class when_merging_ptables_with_entries_to_nonexisting_record : IClassFixture<when_merging_ptables_with_entries_to_nonexisting_record.FixtureData>
    {
        private PTable _newtable;

        public class FixtureData : SpecificationWithDirectoryPerTestFixture
        {
            private readonly List<string> _files = new List<string>();
            private readonly List<PTable> _tables = new List<PTable>();
            public PTable _newtable;

            public FixtureData()
            {
                for (int i = 0; i < 4; i++)
                {
                    _files.Add(GetTempFilePath());

                    var table = new HashListMemTable(maxSize: 30);
                    for (int j = 0; j < 10; j++)
                    {
                        table.Add((uint)i, j, i * 10 + j);
                    }
                    _tables.Add(PTable.FromMemtable(table, _files[i]));
                }
                _files.Add(GetTempFilePath());
                _newtable = PTable.MergeTo(_tables, _files[4], x => x.Position % 2 == 0);    
            }

            public override void Dispose()
            {
                _newtable.Dispose();
                foreach (var ssTable in _tables)
                {
                    ssTable.Dispose();
                }
                base.Dispose();
            }
        }

        public when_merging_ptables_with_entries_to_nonexisting_record(FixtureData data)
        {
            _newtable = data._newtable;
        }

        [Fact]
        public void there_are_only_twenty_entries_left()
        {
            Assert.Equal(20, _newtable.Count);
        }

        [Fact]
        public void the_hash_can_be_verified()
        {
            var ex = Record.Exception(() => _newtable.VerifyFileHash());
            Assert.Null(ex);
        }

        [Fact]
        public void the_items_are_sorted()
        {
            var last = new IndexEntry(ulong.MaxValue, long.MaxValue);
            foreach (var item in _newtable.IterateAllInOrder())
            {
                Assert.True(last.Key > item.Key || last.Key == item.Key && last.Position > item.Position);
                last = item;
            }
        }

        [Fact]
        public void the_right_items_are_deleted()
        {
            for (int i = 0; i < 4; i++)
            {
                for (int j = 0; j < 10; j++)
                {
                    long position;
                    if ((i*10 + j)%2 == 0)
                    {
                        Assert.True(_newtable.TryGetOneValue((uint)i, j, out position));
                        Assert.Equal(i*10+j, position);
                    }
                    else
                        Assert.False(_newtable.TryGetOneValue((uint)i, j, out position));
                }
            }
        }
    }
}