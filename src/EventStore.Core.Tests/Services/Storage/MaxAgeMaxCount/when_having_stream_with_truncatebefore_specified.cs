using EventStore.Core.Data;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount
{
    public class when_having_stream_with_truncatebefore_specified : ReadIndexTestScenario
    {
        private EventRecord _r1;
        private EventRecord _r2;
        private EventRecord _r3;
        private EventRecord _r4;
        private EventRecord _r5;
        private EventRecord _r6;

        protected override void WriteTestScenario()
        {
            const string metadata = @"{""$tb"":2}";

            var r1 = Fixture.WriteStreamMetadata("ES", 0, metadata);
            var r2 = Fixture.WriteSingleEvent("ES", 0, "bla1");
            var r3 = Fixture.WriteSingleEvent("ES", 1, "bla1");
            var r4 = Fixture.WriteSingleEvent("ES", 2, "bla1");
            var r5 = Fixture.WriteSingleEvent("ES", 3, "bla1");
            var r6 = Fixture.WriteSingleEvent("ES", 4, "bla1");

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._r1 = r1;
                instance._r2 = r2;
                instance._r3 = r3;
                instance._r4 = r4;
                instance._r5 = r5;
                instance._r6 = r6;
            });
        }

        [Fact]
        public void single_event_read_doesnt_return_old_events_and_return_actual_ones()
        {
            var result = ReadIndex.ReadEvent("ES", 0);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);

            result = ReadIndex.ReadEvent("ES", 1);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);

            result = ReadIndex.ReadEvent("ES", 2);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_r4, result.Record);

            result = ReadIndex.ReadEvent("ES", 3);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_r5, result.Record);

            result = ReadIndex.ReadEvent("ES", 4);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_r6, result.Record);
        }

        [Fact]
        public void forward_range_read_doesnt_return_old_records()
        {
            var result = ReadIndex.ReadStreamEventsForward("ES", 0, 100);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(3,   result.Records.Length);
            Assert.Equal(_r4, result.Records[0]);
            Assert.Equal(_r5, result.Records[1]);
            Assert.Equal(_r6, result.Records[2]);
        }

        [Fact]
        public void backward_range_read_doesnt_return_expired_records()
        {
            var result = ReadIndex.ReadStreamEventsBackward("ES", -1, 100);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(3, result.Records.Length);
            Assert.Equal(_r6, result.Records[0]);
            Assert.Equal(_r5, result.Records[1]);
            Assert.Equal(_r4, result.Records[2]);
        }

        [Fact]
        public void read_all_forward_returns_all_records_including_expired_ones()
        {
            var records = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records;
            Assert.Equal(6, records.Count);
            Assert.Equal(_r1, records[0].Event);
            Assert.Equal(_r2, records[1].Event);
            Assert.Equal(_r3, records[2].Event);
            Assert.Equal(_r4, records[3].Event);
            Assert.Equal(_r5, records[4].Event);
            Assert.Equal(_r6, records[5].Event);
        }

        [Fact]
        public void read_all_backward_returns_all_records_including_expired_ones()
        {
            var records = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100).Records;
            Assert.Equal(6, records.Count);
            Assert.Equal(_r6, records[0].Event);
            Assert.Equal(_r5, records[1].Event);
            Assert.Equal(_r4, records[2].Event);
            Assert.Equal(_r3, records[3].Event);
            Assert.Equal(_r2, records[4].Event);
            Assert.Equal(_r1, records[5].Event);
        }
    }
}