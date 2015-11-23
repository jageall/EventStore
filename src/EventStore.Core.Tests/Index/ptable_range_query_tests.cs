using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class ptable_range_query_tests:IUseFixture<ptable_range_query_tests.FixtureData>{

        private PTable _ptable;
        public class FixtureData : SpecificationWithFilePerTestFixture
        {
            public readonly PTable _ptable;

            public FixtureData()
            {
                var table = new HashListMemTable(maxSize: 50);
                table.Add(0x0101, 0x0001, 0x0001);
                table.Add(0x0105, 0x0001, 0x0002);
                table.Add(0x0102, 0x0001, 0x0003);
                table.Add(0x0102, 0x0002, 0x0004);
                table.Add(0x0103, 0x0001, 0xFFF1);
                table.Add(0x0103, 0x0003, 0xFFF3);
                table.Add(0x0103, 0x0005, 0xFFF5);
                _ptable = PTable.FromMemtable(table, Filename, cacheDepth: 0);
            }

            public override void Dispose()
            {
                _ptable.Dispose();
                base.Dispose();
            }
        }

        public void SetFixture(FixtureData data)
        {
            _ptable = data._ptable;
        }

        [Fact]
        public void range_query_of_non_existing_stream_returns_nothing()
        {
            var list = _ptable.GetRange(0x14, 0x01, 0x02).ToArray();
            Assert.Equal(0, list.Length);
        }

        [Fact]
        public void range_query_of_non_existing_version_returns_nothing()
        {
            var list = _ptable.GetRange(0x0101, 0x03, 0x05).ToArray();
            Assert.Equal(0, list.Length);
        }

        [Fact]
        public void range_query_with_hole_returns_items_included()
        {
            var list = _ptable.GetRange(0x0103, 0x01, 0x05).ToArray();
            Assert.Equal(3, list.Length);
            Assert.Equal(0x0103u, list[0].Stream);
            Assert.Equal(0x05, list[0].Version);
            Assert.Equal(0xfff5, list[0].Position);
            Assert.Equal(0x0103u, list[1].Stream);
            Assert.Equal(0x03, list[1].Version);
            Assert.Equal(0xfff3, list[1].Position);
            Assert.Equal(0x0103u, list[2].Stream);
            Assert.Equal(0x01, list[2].Version);
            Assert.Equal(0xfff1, list[2].Position);
        }

        [Fact]
        public void query_with_start_in_range_but_not_end_results_returns_items_included()
        {
            var list = _ptable.GetRange(0x0103, 0x01, 0x04).ToArray();
            Assert.Equal(2, list.Length);
            Assert.Equal(0x0103u, list[0].Stream);
            Assert.Equal(0x03, list[0].Version);
            Assert.Equal(0xfff3, list[0].Position);
            Assert.Equal(0x0103u, list[1].Stream);
            Assert.Equal(0x01, list[1].Version);
            Assert.Equal(0xfff1, list[1].Position);
        }

        [Fact]
        public void query_with_end_in_range_but_not_start_results_returns_items_included()
        {
            var list = _ptable.GetRange(0x0103, 0x00, 0x03).ToArray();
            Assert.Equal(2, list.Length);
            Assert.Equal(0x0103u, list[0].Stream);
            Assert.Equal(0x03, list[0].Version);
            Assert.Equal(0xfff3, list[0].Position);
            Assert.Equal(0x0103u, list[1].Stream);
            Assert.Equal(0x01, list[1].Version);
            Assert.Equal(0xfff1, list[1].Position);
        }

        [Fact]
        public void query_with_end_and_start_exclusive_results_returns_items_included()
        {
            var list = _ptable.GetRange(0x0103, 0x00, 0x06).ToArray();
            Assert.Equal(3, list.Length);
            Assert.Equal(0x0103u, list[0].Stream);
            Assert.Equal(0x05, list[0].Version);
            Assert.Equal(0xfff5, list[0].Position);
            Assert.Equal(0x0103u, list[1].Stream);
            Assert.Equal(0x03, list[1].Version);
            Assert.Equal(0xfff3, list[1].Position);
            Assert.Equal(0x0103u, list[2].Stream);
            Assert.Equal(0x01, list[2].Version);
            Assert.Equal(0xfff1, list[2].Position);
        }

        [Fact]
        public void query_with_end_inside_the_hole_in_list_returns_items_included()
        {
            var list = _ptable.GetRange(0x0103, 0x00, 0x04).ToArray();
            Assert.Equal(2, list.Length);
            Assert.Equal(0x0103u, list[0].Stream);
            Assert.Equal(0x03, list[0].Version);
            Assert.Equal(0xfff3, list[0].Position);
            Assert.Equal(0x0103u, list[1].Stream);
            Assert.Equal(0x01, list[1].Version);
            Assert.Equal(0xfff1, list[1].Position);
        }

        [Fact]
        public void query_with_start_inside_the_hole_in_list_returns_items_included()
        {
            var list = _ptable.GetRange(0x0103, 0x02, 0x06).ToArray();
            Assert.Equal(2, list.Length);
            Assert.Equal(0x0103u, list[0].Stream);
            Assert.Equal(0x05, list[0].Version);
            Assert.Equal(0xfff5, list[0].Position);
            Assert.Equal(0x0103u, list[1].Stream);
            Assert.Equal(0x03, list[1].Version);
            Assert.Equal(0xfff3, list[1].Position);
        }

        [Fact]
        public void query_with_start_and_end_inside_the_hole_in_list_returns_items_included()
        {
            var list = _ptable.GetRange(0x0103, 0x02, 0x04).ToArray();
            Assert.Equal(1, list.Length);
            Assert.Equal(0x0103u, list[0].Stream);
            Assert.Equal(0x03, list[0].Version);
            Assert.Equal(0xfff3, list[0].Position);
        }

        [Fact]
        public void query_with_start_and_end_less_than_all_items_returns_nothing()
        {
            var list = _ptable.GetRange(0x0103, 0x00, 0x00).ToArray();
            Assert.Equal(0, list.Length);
        }

        [Fact]
        public void query_with_start_and_end_greater_than_all_items_returns_nothing()
        {
            var list = _ptable.GetRange(0x0103, 0x06, 0x06).ToArray();
            Assert.Equal(0, list.Length);
        }
    }
}