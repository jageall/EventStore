using System;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.BuildingIndex
{
    public class when_building_an_index_off_tfile_with_prepares_and_commits : ReadIndexTestScenario
    {
        private Guid _id1;
        private Guid _id2;
        private Guid _id3;

        protected override void WriteTestScenario()
        {
            var id1 = _id1 = Guid.NewGuid();
            var id2 = _id2 = Guid.NewGuid();
            var id3 = _id3 = Guid.NewGuid();
            long pos1, pos2, pos3, pos4, pos5, pos6;
            Fixture.Writer.Write(new PrepareLogRecord(0, _id1, _id1, 0, 0, "test1", ExpectedVersion.Any, DateTime.UtcNow,
                                              PrepareFlags.SingleWrite, "type", new byte[0], new byte[0]),
                         out pos1);
            Fixture.Writer.Write(new PrepareLogRecord(pos1, _id2, _id2, pos1, 0, "test2", ExpectedVersion.Any, DateTime.UtcNow,
                                              PrepareFlags.SingleWrite, "type", new byte[0], new byte[0]),
                         out pos2);
            Fixture.Writer.Write(new PrepareLogRecord(pos2, _id3, _id3, pos2, 0, "test2", ExpectedVersion.Any, DateTime.UtcNow,
                                              PrepareFlags.SingleWrite, "type", new byte[0], new byte[0]),
                         out pos3);
            Fixture.Writer.Write(new CommitLogRecord(pos3, _id1, 0, DateTime.UtcNow, 0), out pos4);
            Fixture.Writer.Write(new CommitLogRecord(pos4, _id2, pos1, DateTime.UtcNow, 0), out pos5);
            Fixture.Writer.Write(new CommitLogRecord(pos5, _id3, pos2, DateTime.UtcNow, 1), out pos6);

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._id1 = id1;
                instance._id2 = id2;
                instance._id3 = id3;
            });
        }

        [Fact]
        public void the_first_event_can_be_read()
        {
            var result = ReadIndex.ReadEvent("test1", 0);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_id1, result.Record.EventId);
        }

        [Fact]
        public void the_nonexisting_event_can_not_be_read()
        {
            var result = ReadIndex.ReadEvent("test1", 1);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);
        }

        [Fact]
        public void the_second_event_can_be_read()
        {
            var result = ReadIndex.ReadEvent("test2", 0);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_id2, result.Record.EventId);
        }

        [Fact]
        public void the_last_event_of_first_stream_can_be_read()
        {
            var result = ReadIndex.ReadEvent("test1", -1);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_id1, result.Record.EventId);
        }

        [Fact]
        public void the_last_event_of_second_stream_can_be_read()
        {
            var result = ReadIndex.ReadEvent("test2", -1);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_id3, result.Record.EventId);
        }

        [Fact]
        public void the_last_event_of_nonexistent_stream_cant_be_read()
        {
            var result = ReadIndex.ReadEvent("test7", -1);
            Assert.Equal(ReadEventResult.NoStream, result.Result);
            Assert.Null(result.Record);
        }

        [Fact]
        public void the_stream_can_be_read_for_first_stream()
        {
            var result = ReadIndex.ReadStreamEventsBackward("test1", 0, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(_id1, result.Records[0].EventId);
        }

        [Fact]
        public void the_stream_can_be_read_for_second_stream_from_end()
        {
            var result = ReadIndex.ReadStreamEventsBackward("test2", -1, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(_id3, result.Records[0].EventId);
        }

        [Fact]
        public void the_stream_can_be_read_for_second_stream_from_event_number()
        {
            var result = ReadIndex.ReadStreamEventsBackward("test2", 1, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(_id3, result.Records[0].EventId);
        }
    
        [Fact]
        public void read_all_events_forward_returns_all_events_in_correct_order()
        {
            var records = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 10).Records;

            Assert.Equal(3, records.Count);
            Assert.Equal(_id1, records[0].Event.EventId);
            Assert.Equal(_id2, records[1].Event.EventId);
            Assert.Equal(_id3, records[2].Event.EventId);
        }

        [Fact]
        public void read_all_events_backward_returns_all_events_in_correct_order()
        {
            var records = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 10).Records;

            Assert.Equal(3, records.Count);
            Assert.Equal(_id1, records[2].Event.EventId);
            Assert.Equal(_id2, records[1].Event.EventId);
            Assert.Equal(_id3, records[0].Event.EventId);
        }

        public when_building_an_index_off_tfile_with_prepares_and_commits(FixtureData fixture) : base(fixture)
        {
        }
    }
}