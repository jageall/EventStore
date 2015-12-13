using System;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.BuildingIndex
{
    public class when_building_an_index_off_tfile_with_two_events_in_stream : ReadIndexTestScenario
    {
        private Guid _id1;
        private Guid _id2;

        private PrepareLogRecord _prepare1;
        private PrepareLogRecord _prepare2;

        protected override void WriteTestScenario()
        {
            var id1 = _id1 = Guid.NewGuid();
            var id2 = _id2 = Guid.NewGuid();

            long pos1, pos2, pos3, pos4;
            var prepare1 = new PrepareLogRecord(0, _id1, _id1, 0, 0, "test1", ExpectedVersion.NoStream, DateTime.UtcNow,
                                             PrepareFlags.SingleWrite, "type", new byte[0], new byte[0]);
            Fixture.Writer.Write(prepare1, out pos1);
            var prepare2 = new PrepareLogRecord(pos1, _id2, _id2, pos1, 0, "test1", 0, DateTime.UtcNow,
                                             PrepareFlags.SingleWrite, "type", new byte[0], new byte[0]);
            Fixture.Writer.Write(prepare2, out pos2);
            Fixture.Writer.Write(new CommitLogRecord(pos2, _id1, 0, DateTime.UtcNow, 0), out pos3);
            Fixture.Writer.Write(new CommitLogRecord(pos3, _id2, pos1, DateTime.UtcNow, 1), out pos4);

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._id1 = id1;
                instance._id2 = id2;
                instance._prepare1 = prepare1;
                instance._prepare2 = prepare2;
            });

        }

        [Fact]
        public void the_first_event_can_be_read()
        {
            var result = ReadIndex.ReadEvent("test1", 0);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(new EventRecord(0, _prepare1), result.Record);
        }

        [Fact]
        public void the_second_event_can_be_read()
        {
            var result = ReadIndex.ReadEvent("test1", 1);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(new EventRecord(1, _prepare2), result.Record);
        }

        [Fact]
        public void the_nonexisting_event_can_not_be_read()
        {
            var result = ReadIndex.ReadEvent("test1", 2);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
        }

        [Fact]
        public void the_last_event_can_be_read_and_is_correct()
        {
            var result = ReadIndex.ReadEvent("test1", -1);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(new EventRecord(1, _prepare2), result.Record);
        }

        [Fact]
        public void the_first_event_can_be_read_through_range_query()
        {
            var result = ReadIndex.ReadStreamEventsBackward("test1", 0, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(new EventRecord(0, _prepare1), result.Records[0]);
        }

        [Fact]
        public void the_second_event_can_be_read_through_range_query()
        {
            var result = ReadIndex.ReadStreamEventsBackward("test1", 1, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(new EventRecord(1, _prepare2), result.Records[0]);
        }

        [Fact]
        public void the_stream_can_be_read_as_a_whole_with_specific_from_version()
        {
            var result = ReadIndex.ReadStreamEventsBackward("test1", 1, 2);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(2, result.Records.Length);
            Assert.Equal(new EventRecord(1, _prepare2), result.Records[0]);
            Assert.Equal(new EventRecord(0, _prepare1), result.Records[1]);
        }

        [Fact]
        public void the_stream_can_be_read_as_a_whole_with_from_end()
        {
            var result = ReadIndex.ReadStreamEventsBackward("test1", -1, 2);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(2, result.Records.Length);
            Assert.Equal(new EventRecord(1, _prepare2), result.Records[0]);
            Assert.Equal(new EventRecord(0, _prepare1), result.Records[1]);
        }

        [Fact]
        public void the_stream_cant_be_read_for_second_stream()
        {
            var result = ReadIndex.ReadStreamEventsBackward("test2", 0, 1);
            Assert.Equal(ReadStreamResult.NoStream, result.Result);
            Assert.Equal(0, result.Records.Length);
        }

        [Fact]
        public void read_all_events_forward_returns_all_events_in_correct_order()
        {
            var records = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 10).Records;

            Assert.Equal(2, records.Count);
            Assert.Equal(_id1, records[0].Event.EventId);
            Assert.Equal(_id2, records[1].Event.EventId);
        }

        [Fact]
        public void read_all_events_backward_returns_all_events_in_correct_order()
        {
            var records = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 10).Records;

            Assert.Equal(2, records.Count);
            Assert.Equal(_id1, records[1].Event.EventId);
            Assert.Equal(_id2, records[0].Event.EventId);
        }

        public when_building_an_index_off_tfile_with_two_events_in_stream(FixtureData fixture) : base(fixture)
        {
        }
    }
}