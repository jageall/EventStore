using System;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount
{
    public class when_having_one_stream_with_maxage_and_other_stream_with_maxcount_and_streams_have_same_hash : ReadIndexTestScenario
    {
        private EventRecord _r11;
        private EventRecord _r12;
        private EventRecord _r13;
        private EventRecord _r14;
        private EventRecord _r15;
        private EventRecord _r16;

        private EventRecord _r21;
        private EventRecord _r22;
        private EventRecord _r23;
        private EventRecord _r24;
        private EventRecord _r25;
        private EventRecord _r26;

        protected override void WriteTestScenario()
        {
            var now = DateTime.UtcNow;

            var metadata1 = string.Format(@"{{""$maxAge"":{0}}}", (int)TimeSpan.FromMinutes(25).TotalSeconds);
            const string metadata2 = @"{""$maxCount"":2}";

            var r11 = Fixture.WriteStreamMetadata("ES1", 0, metadata1);
            var r21 = Fixture.WriteStreamMetadata("ES2", 0, metadata2);
            
            var r12 = Fixture.WriteSingleEvent("ES1", 0, "bla1", now.AddMinutes(-100));
            var r13 = Fixture.WriteSingleEvent("ES1", 1, "bla1", now.AddMinutes(-20));
            
            var r22 = Fixture.WriteSingleEvent("ES2", 0, "bla1", now.AddMinutes(-100));
            var r23 = Fixture.WriteSingleEvent("ES2", 1, "bla1", now.AddMinutes(-20));
            
            var r14 = Fixture.WriteSingleEvent("ES1", 2, "bla1", now.AddMinutes(-11));
            var r24 = Fixture.WriteSingleEvent("ES2", 2, "bla1", now.AddMinutes(-10));
            
            var r15 = Fixture.WriteSingleEvent("ES1", 3, "bla1", now.AddMinutes(-5));
            var r16 = Fixture.WriteSingleEvent("ES1", 4, "bla1", now.AddMinutes(-2));
            
            var r25 = Fixture.WriteSingleEvent("ES2", 3, "bla1", now.AddMinutes(-1));
            var r26 = Fixture.WriteSingleEvent("ES2", 4, "bla1", now.AddMinutes(-1));

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._r11 = r11;
                instance._r12 = r12;
                instance._r13 = r13;
                instance._r14 = r14;
                instance._r15 = r15;
                instance._r16 = r16;

                instance._r21 = r21;
                instance._r22 = r22;
                instance._r23 = r23;
                instance._r24 = r24;
                instance._r25 = r25;
                instance._r26 = r26;
            });
        }

        [Fact]
        public void single_event_read_doesnt_return_stream_created_event_for_both_streams()
        {
            var result = ReadIndex.ReadEvent("ES1", 0);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);

            result = ReadIndex.ReadEvent("ES2", 0);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);
        }

        [Fact]
        public void single_event_read_doesnt_return_expired_events_and_returns_all_actual_ones_for_stream_1()
        {
            var result = ReadIndex.ReadEvent("ES1", 0);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);

            result = ReadIndex.ReadEvent("ES1", 1);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_r13, result.Record);

            result = ReadIndex.ReadEvent("ES1", 2);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_r14, result.Record);

            result = ReadIndex.ReadEvent("ES1", 3);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_r15, result.Record);

            result = ReadIndex.ReadEvent("ES1", 4);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_r16, result.Record);
        }

        [Fact]
        public void single_event_read_doesnt_return_expired_events_and_returns_all_actual_ones_for_stream_2()
        {
            var result = ReadIndex.ReadEvent("ES2", 0);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);

            result = ReadIndex.ReadEvent("ES2", 1);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);

            result = ReadIndex.ReadEvent("ES2", 2);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);

            result = ReadIndex.ReadEvent("ES2", 3);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_r25, result.Record);

            result = ReadIndex.ReadEvent("ES2", 4);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_r26, result.Record);
        }

        [Fact]
        public void forward_range_read_doesnt_return_expired_records_for_stream_1()
        {
            var result = ReadIndex.ReadStreamEventsForward("ES1", 0, 100);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(4, result.Records.Length);
            Assert.Equal(_r13, result.Records[0]);
            Assert.Equal(_r14, result.Records[1]);
            Assert.Equal(_r15, result.Records[2]);
            Assert.Equal(_r16, result.Records[3]);
        }

        [Fact]
        public void forward_range_read_doesnt_return_expired_records_for_stream_2()
        {
            var result = ReadIndex.ReadStreamEventsForward("ES2", 0, 100);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(2, result.Records.Length);
            Assert.Equal(_r25, result.Records[0]);
            Assert.Equal(_r26, result.Records[1]);
        }

        [Fact]
        public void backward_range_read_doesnt_return_expired_records_for_stream_1()
        {
            var result = ReadIndex.ReadStreamEventsBackward("ES1", -1, 100);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(4, result.Records.Length);
            Assert.Equal(_r16, result.Records[0]);
            Assert.Equal(_r15, result.Records[1]);
            Assert.Equal(_r14, result.Records[2]);
            Assert.Equal(_r13, result.Records[3]);
        }

        [Fact]
        public void backward_range_read_doesnt_return_expired_records_for_stream_2()
        {
            var result = ReadIndex.ReadStreamEventsBackward("ES2", -1, 100);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(2, result.Records.Length);
            Assert.Equal(_r26, result.Records[0]);
            Assert.Equal(_r25, result.Records[1]);
        }

        [Fact]
        public void read_all_forward_returns_all_records_including_expired_ones()
        {
            var records = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records;
            Assert.Equal(12, records.Count);
            Assert.Equal(_r11, records[0].Event);
            Assert.Equal(_r21, records[1].Event);

            Assert.Equal(_r12, records[2].Event);
            Assert.Equal(_r13, records[3].Event);

            Assert.Equal(_r22, records[4].Event);
            Assert.Equal(_r23, records[5].Event);
            
            Assert.Equal(_r14, records[6].Event);
            Assert.Equal(_r24, records[7].Event);

            Assert.Equal(_r15, records[8].Event);
            Assert.Equal(_r16, records[9].Event);

            Assert.Equal(_r25, records[10].Event);
            Assert.Equal(_r26, records[11].Event);
        }

        [Fact]
        public void read_all_backward_returns_all_records_including_expired_ones()
        {
            var records = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100).Records;
            Assert.Equal(12, records.Count);
            Assert.Equal(_r11, records[11].Event);
            Assert.Equal(_r21, records[10].Event);

            Assert.Equal(_r12, records[9].Event);
            Assert.Equal(_r13, records[8].Event);

            Assert.Equal(_r22, records[7].Event);
            Assert.Equal(_r23, records[6].Event);

            Assert.Equal(_r14, records[5].Event);
            Assert.Equal(_r24, records[4].Event);

            Assert.Equal(_r15, records[3].Event);
            Assert.Equal(_r16, records[2].Event);

            Assert.Equal(_r25, records[1].Event);
            Assert.Equal(_r26, records[0].Event);
        }
    }
}