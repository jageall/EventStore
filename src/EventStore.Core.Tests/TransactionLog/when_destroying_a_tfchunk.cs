using System.IO;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_destroying_a_tfchunk: SpecificationWithFile
    {
        private TFChunk _chunk;

        public when_destroying_a_tfchunk()
        {
            _chunk = TFChunk.CreateNew(Filename, 1000, 0, 0, false);
            _chunk.MarkForDeletion();
        }

        [Fact]
        public void the_file_is_deleted()
        {
            Assert.False(File.Exists(Filename));
        }
    }
}