using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class when_trying_to_get_oldest_entry: SpecificationWithFile
    {
        [Fact]
        public void nothing_is_found_on_empty_stream()
        {
            var memTable = new HashListMemTable(maxSize: 10);
            memTable.Add(0x11, 0x01, 0xffff);
            using (var ptable = PTable.FromMemtable(memTable, Filename))
            {
                IndexEntry entry;
                Assert.False(ptable.TryGetOldestEntry(0x12, out entry));
            }
        }

        [Fact]
        public void single_item_is_latest()
        {
            var memTable = new HashListMemTable(maxSize: 10);
            memTable.Add(0x11, 0x01, 0xffff);
            using (var ptable = PTable.FromMemtable(memTable, Filename))
            {
                IndexEntry entry;
                Assert.True(ptable.TryGetOldestEntry(0x11, out entry));
                Assert.Equal(0x11u, entry.Stream);
                Assert.Equal(0x01, entry.Version);
                Assert.Equal(0xffff, entry.Position);
            }
        }

        [Fact]
        public void correct_entry_is_returned()
        {
            var memTable = new HashListMemTable(maxSize: 10);
            memTable.Add(0x11, 0x01, 0xffff);
            memTable.Add(0x11, 0x02, 0xfff2);
            using (var ptable = PTable.FromMemtable(memTable, Filename))
            {
                IndexEntry entry;
                Assert.True(ptable.TryGetOldestEntry(0x11, out entry));
                Assert.Equal(0x11u, entry.Stream);
                Assert.Equal(0x01, entry.Version);
                Assert.Equal(0xffff, entry.Position);
            }
        }

        [Fact]
        public void when_duplicated_entries_exist_the_one_with_oldest_position_is_returned()
        {
            var memTable = new HashListMemTable(maxSize: 10);
            memTable.Add(0x11, 0x01, 0xfff1);
            memTable.Add(0x11, 0x02, 0xfff2);
            memTable.Add(0x11, 0x01, 0xfff3);
            memTable.Add(0x11, 0x02, 0xfff4);
            using (var ptable = PTable.FromMemtable(memTable, Filename))
            {
                IndexEntry entry;
                Assert.True(ptable.TryGetOldestEntry(0x11, out entry));
                Assert.Equal(0x11u, entry.Stream);
                Assert.Equal(0x01, entry.Version);
                Assert.Equal(0xfff1, entry.Position);
            }
        }

        [Fact]
        public void only_entry_with_smallest_position_is_returned_when_triduplicated()
        {
            var memTable = new HashListMemTable(maxSize: 10);
            memTable.Add(0x11, 0x01, 0xfff1);
            memTable.Add(0x11, 0x01, 0xfff3);
            memTable.Add(0x11, 0x01, 0xfff5);
            using (var ptable = PTable.FromMemtable(memTable, Filename))
            {
                IndexEntry entry;
                Assert.True(ptable.TryGetOldestEntry(0x11, out entry));
                Assert.Equal(0x11u, entry.Stream);
                Assert.Equal(0x01, entry.Version);
                Assert.Equal(0xfff1, entry.Position);
            }
        }
    }
}
