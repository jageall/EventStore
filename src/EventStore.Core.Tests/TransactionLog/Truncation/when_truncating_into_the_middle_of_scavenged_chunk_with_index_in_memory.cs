using System.IO;
using EventStore.Core.Data;
using EventStore.Core.Tests.Services.Storage;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation
{
    public class when_truncating_into_the_middle_of_scavenged_chunk_with_index_in_memory : TruncateScenario
    {
        private string _chunk0;
        private string _chunk1;
        private string _chunk2;
        private string _chunk3;

        private EventRecord _chunkEdge;

        protected override void WriteTestScenario()
        {

            Fixture.WriteSingleEvent("ES1", 0, new string('.', 3000));                    // chunk 0
            Fixture.WriteSingleEvent("ES1", 1, new string('.', 3000));
            Fixture.WriteSingleEvent("ES2", 0, new string('.', 3000));
            var chunkEdge = Fixture.WriteSingleEvent("ES1", 2, new string('.', 3000), retryOnFail: true); // chunk 1
            var ackRec = Fixture.WriteSingleEvent("ES1", 3, new string('.', 3000));
            Fixture.WriteSingleEvent("ES1", 4, new string('.', 3000));
            Fixture.WriteSingleEvent("ES1", 5, new string('.', 3000), retryOnFail: true);  // chunk 2
            Fixture.WriteSingleEvent("ES1", 6, new string('.', 3000));
            Fixture.WriteSingleEvent("ES1", 7, new string('.', 3000));
            Fixture.WriteSingleEvent("ES1", 8, new string('.', 3000), retryOnFail: true);  // chunk 3

            Fixture.WriteDelete("ES1");
            Fixture.Scavenge(completeLast: false, mergeChunks: false);

            TruncateCheckpoint = ackRec.LogPosition;
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._chunkEdge = chunkEdge;
            });
        }

        protected override void OnBeforeTruncating()
        {
            // scavenged chunk names
            // TODO MM: avoid this complexity - try scavenging exactly at where its invoked and not wait for readIndex to rebuild
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
        public void checksums_should_be_equal_to_beginning_of_intersected_scavenged_chunk()
        {
            Assert.Equal(_chunkEdge.TransactionPosition, WriterCheckpoint.Read());
            Assert.Equal(_chunkEdge.TransactionPosition, ChaserCheckpoint.Read());
        }

        [Fact]
        public void truncated_chunks_should_be_deleted()
        {
            Assert.False(File.Exists(_chunk2));
            Assert.False(File.Exists(_chunk3));
        }

        [Fact]
        public void intersecting_chunk_should_be_deleted()
        {
            Assert.False(File.Exists(_chunk1));
        }

        [Fact]
        public void untouched_chunk_should_survive()
        {
            var chunks = Db.Config.FileNamingStrategy.GetAllPresentFiles();
            Assert.Equal(1, chunks.Length);
            Assert.Equal(_chunk0, GetChunkName(0));
        }

        public when_truncating_into_the_middle_of_scavenged_chunk_with_index_in_memory(FixtureData fixture) : base(fixture)
        {
        }
    }
}