using System.IO;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class destroying_ptable: SpecificationWithFile
    {
        private PTable _table;

        public destroying_ptable()
        {
            var mtable = new HashListMemTable(maxSize: 10);
            mtable.Add(0x0101, 0x0001, 0x0001);
            mtable.Add(0x0105, 0x0001, 0x0002);
            _table = PTable.FromMemtable(mtable, Filename);
            _table.MarkForDestruction();
        }

        [Fact]
        public void the_file_is_deleted()
        {
            _table.WaitForDisposal(1000);
            Assert.False(File.Exists(Filename));
        }

        [Fact]
        public void wait_for_destruction_returns()
        {
            var ex = Record.Exception(() => _table.WaitForDisposal(1000));
            Assert.Null(ex);
        }

        public override void Dispose()
        {
            _table.WaitForDisposal(1000);
            File.Delete(Filename);
            base.Dispose();
        }
    }
}