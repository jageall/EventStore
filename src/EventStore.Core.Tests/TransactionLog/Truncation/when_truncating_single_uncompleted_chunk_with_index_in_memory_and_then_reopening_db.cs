using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Tests.Services.Storage;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation
{
    public class when_truncating_single_uncompleted_chunk_with_index_in_memory_and_then_reopening_db : TruncateAndReOpenDbScenario
    {
        private EventRecord _event1;
        private EventRecord _event2;
        private EventRecord _event3;

        public when_truncating_single_uncompleted_chunk_with_index_in_memory_and_then_reopening_db()
            : base(20000)
        {
        }

        protected override void WriteTestScenario()
        {
            var event1 = Fixture.WriteSingleEvent("ES", 0, new string('.', 500));
            var event2 = Fixture.WriteSingleEvent("ES", 1, new string('.', 500));    // truncated
            var event3 = Fixture.WriteSingleEvent("ES", 2, new string('.', 500));    // truncated

            TruncateCheckpoint = event2.LogPosition;

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._event1 = event1;
                instance._event2 = event2;
                instance._event3 = event3;
            });
        }

        [Fact]
        public void checksums_should_be_equal_to_ack_checksum()
        {
            Assert.Equal(TruncateCheckpoint, WriterCheckpoint.Read());
            Assert.Equal(TruncateCheckpoint, ChaserCheckpoint.Read());
        }

        [Fact]
        public void read_one_by_one_doesnt_return_truncated_records()
        {
            var res = ReadIndex.ReadEvent("ES", 0);
            Assert.Equal(ReadEventResult.Success, res.Result);
            Assert.Equal(_event1, res.Record);

            res = ReadIndex.ReadEvent("ES", 1);
            Assert.Equal(ReadEventResult.NotFound, res.Result);
            Assert.Null(res.Record);

            res = ReadIndex.ReadEvent("ES", 2);
            Assert.Equal(ReadEventResult.NotFound, res.Result);
            Assert.Null(res.Record);

            res = ReadIndex.ReadEvent("ES", 3);
            Assert.Equal(ReadEventResult.NotFound, res.Result);
            Assert.Null(res.Record);
        }

        [Fact]
        public void read_stream_forward_doesnt_return_truncated_records()
        {
            var res = ReadIndex.ReadStreamEventsForward("ES", 0, 100);
            var records = res.Records;
            Assert.Equal(1, records.Length);
            Assert.Equal(_event1, records[0]);
        }

        [Fact]
        public void read_stream_backward_doesnt_return_truncated_records()
        {
            var res = ReadIndex.ReadStreamEventsBackward("ES", -1, 100);
            var records = res.Records;
            Assert.Equal(1, records.Length);
            Assert.Equal(_event1, records[0]);
        }

        [Fact]
        public void read_all_forward_doesnt_return_truncated_records()
        {
            var res = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100);
            var records = res.Records.Select(r => r.Event).ToArray();
            Assert.Equal(1, records.Length);
            Assert.Equal(_event1, records[0]);
        }

        [Fact]
        public void read_all_backward_doesnt_return_truncated_records()
        {
            var res = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100);
            var records = res.Records.Select(r => r.Event).ToArray();
            Assert.Equal(1, records.Length);
            Assert.Equal(_event1, records[0]);
        }

        [Fact]
        public void read_all_backward_from_last_truncated_record_returns_no_records()
        {
            var pos = new TFPos(_event3.LogPosition, _event3.LogPosition);
            var res = ReadIndex.ReadAllEventsForward(pos, 100);
            var records = res.Records.Select(r => r.Event).ToArray();
            Assert.Equal(0, records.Length);
        }
    }
}