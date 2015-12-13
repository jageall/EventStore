using EventStore.Core.Data;
using EventStore.Core.Tests.Services.Storage;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation
{
    public class when_truncating_single_uncompleted_chunk_with_index_on_disk : TruncateScenario
    {
        private EventRecord _event2;

        public when_truncating_single_uncompleted_chunk_with_index_on_disk(FixtureData fixture)
            : base(fixture, maxEntriesInMemTable: 3, metastreamMaxCount: 1)
        {
        }

        protected override void WriteTestScenario()
        {
            Fixture.WriteSingleEvent("ES", 0, new string('.', 500));
            _event2 = Fixture.WriteSingleEvent("ES", 1, new string('.', 500));
            Fixture.WriteSingleEvent("ES", 2, new string('.', 500));  // index goes to disk
            Fixture.WriteSingleEvent("ES", 3, new string('.', 500));

            TruncateCheckpoint = _event2.LogPosition;
        }

        [Fact]
        public void checksums_should_be_equal_to_ack_checksum()
        {
            Assert.Equal(TruncateCheckpoint, WriterCheckpoint.Read());
            Assert.Equal(TruncateCheckpoint, ChaserCheckpoint.Read());
        }
    }
}