using System.Collections.Generic;
using System.Linq;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Data;
using EventStore.Core.Services;
using EventStore.Core.Tests.ClientAPI.Helpers;
using Xunit;
using ExpectedVersion = EventStore.ClientAPI.ExpectedVersion;
using StreamMetadata = EventStore.ClientAPI.StreamMetadata;

namespace EventStore.Core.Tests.ClientAPI
{
    public class read_all_events_forward_with_soft_deleted_stream_should : SpecificationWithMiniNode
    {
        private EventData[] _testEvents;

        protected override void When()
        {
            _conn.SetStreamMetadataAsync(
                "$all", -1, StreamMetadata.Build().SetReadRole(SystemRoles.All),
                new UserCredentials(SystemUsers.Admin, SystemUsers.DefaultAdminPassword))
            .Wait();

            var testEvents = Enumerable.Range(0, 20).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
            _conn.AppendToStreamAsync("stream", ExpectedVersion.EmptyStream, testEvents).Wait();
            _conn.DeleteStreamAsync("stream", ExpectedVersion.Any).Wait();
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._testEvents = testEvents;
            });
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void ensure_deleted_stream()
        {
            var res = _conn.ReadStreamEventsForwardAsync("stream", 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.StreamNotFound, res.Status);
            Assert.Equal(0, res.Events.Length);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_all_events_including_tombstone()
        {
            AllEventsSlice read = _conn.ReadAllEventsForwardAsync(Position.Start, _testEvents.Length + 10, false).Result;
            Assert.True(
                EventDataComparer.Equal(
                    _testEvents.ToArray(),
                    read.Events.Skip(read.Events.Length - _testEvents.Length - 1)
                        .Take(_testEvents.Length)
                        .Select(x => x.Event)
                        .ToArray()));
            var lastEvent = read.Events.Last().Event;
            Assert.Equal("$$stream", lastEvent.EventStreamId);
            Assert.Equal(SystemEventTypes.StreamMetadata, lastEvent.EventType);
            var metadata = StreamMetadata.FromJsonBytes(lastEvent.Data);
            Assert.Equal(EventNumber.DeletedStream, metadata.TruncateBefore);
        }
    }
}
