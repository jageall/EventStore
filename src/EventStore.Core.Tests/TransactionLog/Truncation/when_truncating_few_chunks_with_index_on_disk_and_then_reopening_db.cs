using System.IO;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Tests.Services.Storage;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation
{
    public class when_truncating_few_chunks_with_index_on_disk_and_then_reopening_db : TruncateAndReOpenDbScenario
    {
        private EventRecord _event1;
        private EventRecord _event2;
        private EventRecord _event3;
        private EventRecord _event4;
        private EventRecord _event7;

        private string _chunk0;
        private string _chunk1;
        private string _chunk2;
        private string _chunk3;

        public when_truncating_few_chunks_with_index_on_disk_and_then_reopening_db()
            : base(maxEntriesInMemTable: 3)
        {
        }

        protected override void WriteTestScenario()
        {
            var event1 = Fixture.WriteSingleEvent("ES", 0, new string('.', 4000));                     // chunk 0
            var event2 = Fixture.WriteSingleEvent("ES", 1, new string('.', 4000));
            var event3 = Fixture.WriteSingleEvent("ES", 2, new string('.', 4000), retryOnFail: true);  // ptable 1, chunk 1
            var event4 = Fixture.WriteSingleEvent("ES", 3, new string('.', 4000));
                         Fixture.WriteSingleEvent("ES", 4, new string('.', 4000), retryOnFail: true);  // chunk 2
                         Fixture.WriteSingleEvent("ES", 5, new string('.', 4000));  // ptable 2
            var event7 = Fixture.WriteSingleEvent("ES", 6, new string('.', 4000), retryOnFail: true);  // chunk 3 

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
                instance._event1 = event1;
                instance._event2 = event2;
                instance._event3 = event3;
                instance._event4 = event4;
                instance._event7 = event7;
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

        [Fact]
        public void read_one_by_one_doesnt_return_truncated_records()
        {
            var res = ReadIndex.ReadEvent("ES", 0);
            Assert.Equal(ReadEventResult.Success, res.Result);
            Assert.Equal(_event1, res.Record);
            res = ReadIndex.ReadEvent("ES", 1);
            Assert.Equal(ReadEventResult.Success, res.Result);
            Assert.Equal(_event2, res.Record);
            res = ReadIndex.ReadEvent("ES", 2);
            Assert.Equal(ReadEventResult.Success, res.Result);
            Assert.Equal(_event3, res.Record);

            res = ReadIndex.ReadEvent("ES", 3);
            Assert.Equal(ReadEventResult.NotFound, res.Result);
            Assert.Null(res.Record);
            res = ReadIndex.ReadEvent("ES", 4);
            Assert.Equal(ReadEventResult.NotFound, res.Result);
            Assert.Null(res.Record);
            res = ReadIndex.ReadEvent("ES", 5);
            Assert.Equal(ReadEventResult.NotFound, res.Result);
            Assert.Null(res.Record);
            res = ReadIndex.ReadEvent("ES", 6);
            Assert.Equal(ReadEventResult.NotFound, res.Result);
            Assert.Null(res.Record);
            res = ReadIndex.ReadEvent("ES", 7);
            Assert.Equal(ReadEventResult.NotFound, res.Result);
            Assert.Null(res.Record);
        }

        [Fact]
        public void read_stream_forward_doesnt_return_truncated_records()
        {
            var res = ReadIndex.ReadStreamEventsForward("ES", 0, 100);
            var records = res.Records;
            Assert.Equal(3, records.Length);
            Assert.Equal(_event1, records[0]);
            Assert.Equal(_event2, records[1]);
            Assert.Equal(_event3, records[2]);
        }

        [Fact]
        public void read_stream_backward_doesnt_return_truncated_records()
        {
            var res = ReadIndex.ReadStreamEventsBackward("ES", -1, 100);
            var records = res.Records;
            Assert.Equal(3, records.Length);
            Assert.Equal(_event1, records[2]);
            Assert.Equal(_event2, records[1]);
            Assert.Equal(_event3, records[0]);
        }

        [Fact]
        public void read_all_returns_only_survived_events()
        {
            var res = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100);
            var records = res.Records.Select(r => r.Event).ToArray();

            Assert.Equal(3, records.Length);
            Assert.Equal(_event1, records[0]);
            Assert.Equal(_event2, records[1]);
            Assert.Equal(_event3, records[2]);
        }

        [Fact]
        public void read_all_backward_doesnt_return_truncated_records()
        {
            var res = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100);
            var records = res.Records.Select(r => r.Event).ToArray();
            Assert.Equal(3, records.Length);
            Assert.Equal(_event1, records[2]);
            Assert.Equal(_event2, records[1]);
            Assert.Equal(_event3, records[0]);
        }

        [Fact]
        public void read_all_backward_from_last_truncated_record_returns_no_records()
        {
            var pos = new TFPos(_event7.LogPosition, _event3.LogPosition);
            var res = ReadIndex.ReadAllEventsForward(pos, 100);
            var records = res.Records.Select(r => r.Event).ToArray();
            Assert.Equal(0, records.Length);
        }
    }
}