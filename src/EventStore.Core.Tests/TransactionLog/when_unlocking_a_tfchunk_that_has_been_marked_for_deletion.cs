using System.IO;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_unlocking_a_tfchunk_that_has_been_marked_for_deletion: SpecificationWithFile
    {
        private TFChunk _chunk;

        public when_unlocking_a_tfchunk_that_has_been_marked_for_deletion()
        {
            _chunk = TFChunk.CreateNew(Filename, 1000, 0, 0, false);
            var reader = _chunk.AcquireReader();
            _chunk.MarkForDeletion();
            reader.Release();
        }

        [Fact]
        public void the_file_is_deleted()
        {
            Assert.False(File.Exists(Filename));
        }
    }
}