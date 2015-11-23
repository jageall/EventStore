using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class searching_ptable_with_items_spanning_few_cache_segments_and_all_items_in_cache : ptable_read_scenario_with_items_spanning_few_cache_segments
    {
        public searching_ptable_with_items_spanning_few_cache_segments_and_all_items_in_cache()
            : base(midpointCacheDepth: 10)
        {

        }
    }

    public class searching_ptable_with_items_spanning_few_cache_segments_and_only_some_items_in_cache : ptable_read_scenario_with_items_spanning_few_cache_segments
    {
        public searching_ptable_with_items_spanning_few_cache_segments_and_only_some_items_in_cache()
            : base(midpointCacheDepth: 0)
        {
        }
    }

    public abstract class ptable_read_scenario_with_items_spanning_few_cache_segments : PTableReadScenario
    {
        protected ptable_read_scenario_with_items_spanning_few_cache_segments(int midpointCacheDepth)
            : base(midpointCacheDepth)
        {
        }

        protected override void AddItemsForScenario(IMemTable memTable)
        {
            memTable.Add(0, 0, 0x0001);
            memTable.Add(0, 0, 0x0002);
            memTable.Add(1, 0, 0x0003);
            memTable.Add(1, 0, 0x0004);
            memTable.Add(1, 0, 0x0005);
        }

        [Fact]
        public void the_table_has_five_items()
        {
            Assert.Equal(5, PTable.Count);
        }

        [Fact]
        public void the_smallest_items_can_be_found()
        {
            long position;
            Assert.True(PTable.TryGetOneValue(0, 0, out position));
            Assert.Equal(0x0002, position);
        }

        [Fact]
        public void the_smallest_items_are_returned_in_descending_order()
        {
            var entries = PTable.GetRange(0, 0, 0).ToArray();
            Assert.Equal(2, entries.Length);
            Assert.Equal(0u, entries[0].Stream);
            Assert.Equal(0, entries[0].Version);
            Assert.Equal(0x0002, entries[0].Position);
            Assert.Equal(0u, entries[1].Stream);
            Assert.Equal(0, entries[1].Version);
            Assert.Equal(0x0001, entries[1].Position);
        }

        [Fact]
        public void try_get_latest_entry_for_smallest_hash_returns_correct_index_entry()
        {
            IndexEntry entry;
            Assert.True(PTable.TryGetLatestEntry(0, out entry));
            Assert.Equal(0u, entry.Stream);
            Assert.Equal(0, entry.Version);
            Assert.Equal(0x0002, entry.Position);
        }

        [Fact]
        public void try_get_oldest_entry_for_smallest_hash_returns_correct_index_entry()
        {
            IndexEntry entry;
            Assert.True(PTable.TryGetOldestEntry(0, out entry));
            Assert.Equal(0u, entry.Stream);
            Assert.Equal(0, entry.Version);
            Assert.Equal(0x0001, entry.Position);
        }

        [Fact]
        public void the_largest_items_can_be_found()
        {
            long position;
            Assert.True(PTable.TryGetOneValue(1, 0, out position));
            Assert.Equal(0x0005, position);
        }

        [Fact]
        public void the_largest_items_are_returned_in_descending_order()
        {
            var entries = PTable.GetRange(1, 0, 0).ToArray();
            Assert.Equal(3, entries.Length);
            Assert.Equal(1u, entries[0].Stream);
            Assert.Equal(0, entries[0].Version);
            Assert.Equal(0x0005, entries[0].Position);
            Assert.Equal(1u, entries[1].Stream);
            Assert.Equal(0, entries[1].Version);
            Assert.Equal(0x0004, entries[1].Position);
            Assert.Equal(1u, entries[2].Stream);
            Assert.Equal(0, entries[2].Version);
            Assert.Equal(0x0003, entries[2].Position);
        }

        [Fact]
        public void try_get_latest_entry_for_largest_hash_returns_correct_index_entry()
        {
            IndexEntry entry;
            Assert.True(PTable.TryGetLatestEntry(1, out entry));
            Assert.Equal(1u, entry.Stream);
            Assert.Equal(0, entry.Version);
            Assert.Equal(0x0005, entry.Position);
        }

        [Fact]
        public void try_get_oldest_entry_for_largest_hash_returns_correct_index_entry()
        {
            IndexEntry entry;
            Assert.True(PTable.TryGetOldestEntry(1, out entry));
            Assert.Equal(1u, entry.Stream);
            Assert.Equal(0, entry.Version);
            Assert.Equal(0x0003, entry.Position);
        }

        [Fact]
        public void non_existent_item_cannot_be_found()
        {
            long position;
            Assert.False(PTable.TryGetOneValue(2, 0, out position));
        }

        [Fact]
        public void range_query_returns_nothing_for_nonexistent_stream()
        {
            var entries = PTable.GetRange(2, 0, int.MaxValue).ToArray();
            Assert.Equal(0, entries.Length);
        }

        [Fact]
        public void try_get_latest_entry_returns_nothing_for_nonexistent_stream()
        {
            IndexEntry entry;
            Assert.False(PTable.TryGetLatestEntry(2, out entry));
        }

        [Fact]
        public void try_get_oldest_entry_returns_nothing_for_nonexistent_stream()
        {
            IndexEntry entry;
            Assert.False(PTable.TryGetOldestEntry(2, out entry));
        }
    }
}
