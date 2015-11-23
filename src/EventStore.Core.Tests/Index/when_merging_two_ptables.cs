using System;
using System.Collections.Generic;
using System.IO;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class when_merging_two_ptables : IUseFixture<when_merging_two_ptables.FixtureData>
    {
        private PTable _newtable;

        public class FixtureData : SpecificationWithDirectoryPerTestFixture
        {
            private readonly List<string> _files = new List<string>();
            private readonly List<PTable> _tables = new List<PTable>();

            public PTable _newtable;

            public FixtureData()
            {
                for (int i = 0; i < 2; i++)
                {
                    _files.Add(GetTempFilePath());

                    var table = new HashListMemTable(maxSize: 20);
                    for (int j = 0; j < 10; j++)
                    {
                        table.Add((UInt32) j + 1, i + 1, i*j);
                    }
                    _tables.Add(PTable.FromMemtable(table, _files[i]));
                }
                _files.Add(GetTempFilePath());
                _newtable = PTable.MergeTo(_tables, _files[2], x => true);
            }

            public override void Dispose()
            {
                _newtable.Dispose();
                foreach (var ssTable in _tables)
                {
                    ssTable.Dispose();
                }

            }
        }

        public void SetFixture(FixtureData data)
        {
            _newtable = data._newtable;
        }

        [Fact]
        public void there_are_twenty_records_in_merged_index()
        {
            Assert.Equal(20, _newtable.Count);
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
        public void the_hash_can_be_verified()
        {
            Assert.DoesNotThrow(() => _newtable.VerifyFileHash());
        }
    }
}