using System;
using System.Linq;
using System.Text;
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
    public class read_stream_events_with_unresolved_linkto : SpecificationWithMiniNode
    {
        private EventData[] _testEvents;

        protected override void When()
        {
            _conn.SetStreamMetadataAsync(
                "$all", -1, StreamMetadata.Build().SetReadRole(SystemRoles.All),
                new UserCredentials(SystemUsers.Admin, SystemUsers.DefaultAdminPassword))
            .Wait();

            _testEvents = Enumerable.Range(0, 20).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
            _conn.AppendToStreamAsync("stream", ExpectedVersion.EmptyStream, _testEvents).Wait();
            _conn.AppendToStreamAsync(
                "links", ExpectedVersion.EmptyStream,
                new EventData(
                    Guid.NewGuid(), EventStore.ClientAPI.Common.SystemEventTypes.LinkTo, false,
                    Encoding.UTF8.GetBytes("0@stream"), null))
            .Wait();
            _conn.DeleteStreamAsync("stream", ExpectedVersion.Any).Wait();
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
        public void returns_unresolved_linkto()
        {
            var read = _conn.ReadStreamEventsForwardAsync("links", 0, 1, true).Result;
            Assert.Equal(1, read.Events.Length);
            Assert.Null(read.Events[0].Event);
            Assert.NotNull(read.Events[0].Link);
        }
    }
}
