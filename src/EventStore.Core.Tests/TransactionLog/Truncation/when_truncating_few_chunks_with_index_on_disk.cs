using System.IO;
using EventStore.Core.Data;
using EventStore.Core.Tests.Services.Storage;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation
{
    public class when_truncating_few_chunks_with_index_on_disk : TruncateScenario
    {
        private EventRecord _event4;

        private string _chunk0;
        private string _chunk1;
        private string _chunk2;
        private string _chunk3;

        public when_truncating_few_chunks_with_index_on_disk()
            : base(maxEntriesInMemTable: 3)
        {
        }

        protected override void WriteTestScenario()
        {
            Fixture.WriteSingleEvent("ES", 0, new string('.', 4000));
            Fixture.WriteSingleEvent("ES", 1, new string('.', 4000));
            Fixture.WriteSingleEvent("ES", 2, new string('.', 4000), retryOnFail: true);  // ptable 1, chunk 1
            var event4 = Fixture.WriteSingleEvent("ES", 3, new string('.', 4000));
            Fixture.WriteSingleEvent("ES", 4, new string('.', 4000), retryOnFail: true);  // chunk 2
            Fixture.WriteSingleEvent("ES", 5, new string('.', 4000));  // ptable 2
            Fixture.WriteSingleEvent("ES", 6, new string('.', 4000), retryOnFail: true);  // chunk 3 

            TruncateCheckpoint = event4.LogPosition;

            var chunk0 = GetChunkName(0);
            var chunk1 = GetChunkName(1);
            var chunk2 = GetChunkName(2);
            var chunk3 = GetChunkName(3);

            Assert.True(File.Exists(chunk0));
            Assert.True(File.Exists(chunk1));
            Assert.True(File.Exists(chunk2));
            Assert.True(File.Exists(chunk3));

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._event4 = event4;
                instance._chunk0 = chunk0;
                instance._chunk1 = chunk1;
                instance._chunk2 = chunk2;
                instance._chunk3 = chunk3;
            });
        }

        private string GetChunkName(int chunkNumber)
        {
            var allVersions = Fixture.Db.Config.FileNamingStrategy.GetAllVersionsFor(chunkNumber);
            Assert.Equal(1, allVersions.Length);
            return allVersions[0];
        }

        [Fact]
        public void checksums_should_be_equal_to_ack_checksum()
        {
            Assert.Equal(TruncateCheckpoint, WriterCheckpoint.Read());
            Assert.Equal(TruncateCheckpoint, ChaserCheckpoint.Read());
        }

        [Fact]
        public void truncated_chunks_should_be_deleted()
        {
            Assert.False(File.Exists(_chunk2));
            Assert.False(File.Exists(_chunk3));
        }

        [Fact]
        public void not_truncated_chunks_should_survive()
        {
            var chunks = Db.Config.FileNamingStrategy.GetAllPresentFiles();
            Assert.Equal(2, chunks.Length);
            Assert.Equal(_chunk0, GetChunkName(0));
            Assert.Equal(_chunk1, GetChunkName(1));
        }

        [Fact(Skip = "No asserts")]
        public void read_all_returns_only_survived_events()
        {
        }
    }
}