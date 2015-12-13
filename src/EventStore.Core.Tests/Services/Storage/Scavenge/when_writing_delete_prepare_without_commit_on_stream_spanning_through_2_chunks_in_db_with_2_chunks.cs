using System;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.Scavenge
{
    public class when_writing_delete_prepare_without_commit_and_scavenging : ReadIndexTestScenario
    {
        private EventRecord _event0;
        private EventRecord _event1;

        protected override void WriteTestScenario()
        {
            var event0 = Fixture.WriteSingleEvent("ES", 0, "bla1");

            var prepare = LogRecord.DeleteTombstone(Fixture.WriterCheckpoint.ReadNonFlushed(), Guid.NewGuid(), Guid.NewGuid(), "ES", 2);
            long pos;
            Assert.True(Fixture.Writer.Write(prepare, out pos));

            var event1 = Fixture.WriteSingleEvent("ES", 1, "bla1");
            Fixture.Scavenge(completeLast: false, mergeChunks: false);


            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._event0 = event0;
                instance._event1 = event1;
            });
        }

        [Fact]
        public void read_one_by_one_returns_all_commited_events()
        {
            var result = ReadIndex.ReadEvent("ES", 0);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_event0, result.Record);

            result = ReadIndex.ReadEvent("ES", 1);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_event1, result.Record);
        }

        [Fact]
        public void read_stream_events_forward_should_return_all_events()
        {
            var result = ReadIndex.ReadStreamEventsForward("ES", 0, 100);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(2, result.Records.Length);
            Assert.Equal(_event0, result.Records[0]);
            Assert.Equal(_event1, result.Records[1]);
        }

        [Fact]
        public void read_stream_events_backward_should_return_stream_deleted()
        {
            var result = ReadIndex.ReadStreamEventsBackward("ES", -1, 100);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(2, result.Records.Length);
            Assert.Equal(_event1, result.Records[0]);
            Assert.Equal(_event0, result.Records[1]);
        }

        [Fact]
        public void read_all_forward_returns_all_events()
        {
            var events = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records.Select(r => r.Event).ToArray();
            Assert.Equal(2, events.Length);
            Assert.Equal(_event0, events[0]);
            Assert.Equal(_event1, events[1]);
        }

        [Fact]
        public void read_all_backward_returns_all_events()
        {
            var events = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100).Records.Select(r => r.Event).ToArray();
            Assert.Equal(2, events.Length);
            Assert.Equal(_event1, events[0]);
            Assert.Equal(_event0, events[1]);
        }

        public when_writing_delete_prepare_without_commit_and_scavenging(FixtureData fixture) : base(fixture)
        {
        }
    }
}
