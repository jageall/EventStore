using System.IO;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_destroying_a_tfchunk_that_is_locked: SpecificationWithFile
    {
        private TFChunk _chunk;
        private TFChunkBulkReader _reader;

        public when_destroying_a_tfchunk_that_is_locked()
        {
            _chunk = TFChunk.CreateNew(Filename, 1000, 0, 0, false);
            _reader = _chunk.AcquireReader();
            _chunk.MarkForDeletion();
        }

        public override void Dispose()
        {
            _reader.Release();
            _chunk.MarkForDeletion();
            _chunk.WaitForDestroy(2000);
            base.Dispose();
        }

        [Fact]
        public void the_file_is_not_deleted()
        {
            Assert.True(File.Exists(Filename));
        }
    }
}