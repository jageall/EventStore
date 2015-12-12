using System;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class read_event_should : SpecificationWithMiniNode
    {
        private Guid _eventId0;
        private Guid _eventId1;

        protected override void When()
        {
            var eventId0 = Guid.NewGuid();
            var eventId1 = Guid.NewGuid();

            _conn.AppendToStreamAsync("test-stream",
                                 -1, 
                                 new EventData(eventId0, "event0", false, new byte[3], new byte[2]),
                                 new EventData(eventId1, "event1", true, new byte[7], new byte[10]))
            .Wait();
            _conn.DeleteStreamAsync("deleted-stream", -1, hardDelete: true).Wait();

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._eventId0 = eventId0;
                instance._eventId1 = eventId1;
            });
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public async Task throw_if_stream_id_is_null()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _conn.ReadEventAsync(null, 0, resolveLinkTos: false));
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public async Task throw_if_stream_id_is_empty()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _conn.ReadEventAsync("", 0, resolveLinkTos: false));
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public async Task throw_if_event_number_is_less_than_minus_one()
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _conn.ReadEventAsync("stream", -2, resolveLinkTos: false));
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void notify_using_status_code_if_stream_not_found()
        {
            var res = _conn.ReadEventAsync("unexisting-stream", 5, false).Result;

            Assert.Equal(EventReadStatus.NoStream, res.Status);
            Assert.Null(res.Event);
            Assert.Equal("unexisting-stream", res.Stream);
            Assert.Equal(5, res.EventNumber);
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_no_stream_if_requested_last_event_in_empty_stream()
        {
            var res = _conn.ReadEventAsync("some-really-empty-stream", -1, false).Result;
            Assert.Equal(EventReadStatus.NoStream, res.Status);
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void notify_using_status_code_if_stream_was_deleted()
        {
            var res = _conn.ReadEventAsync("deleted-stream", 5, false).Result;

            Assert.Equal(EventReadStatus.StreamDeleted, res.Status);
            Assert.Null(res.Event);
            Assert.Equal("deleted-stream", res.Stream);
            Assert.Equal(5, res.EventNumber);
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void notify_using_status_code_if_stream_does_not_have_event()
        {
            var res = _conn.ReadEventAsync("test-stream", 5, false).Result;

            Assert.Equal(EventReadStatus.NotFound, res.Status);
            Assert.Null(res.Event);
            Assert.Equal("test-stream", res.Stream);
            Assert.Equal(5, res.EventNumber);
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_existing_event()
        {
            var res = _conn.ReadEventAsync("test-stream", 0, false).Result;

            Assert.Equal(EventReadStatus.Success, res.Status);
            Assert.Equal(res.Event.Value.OriginalEvent.EventId, _eventId0);
            Assert.Equal("test-stream", res.Stream);
            Assert.Equal(0, res.EventNumber);
            Assert.NotEqual(DateTime.MinValue, res.Event.Value.OriginalEvent.Created);
            Assert.NotEqual(0, res.Event.Value.OriginalEvent.CreatedEpoch);
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void retrieve_the_is_json_flag_properly()
        {
            var res = _conn.ReadEventAsync("test-stream", 1, false).Result;

            Assert.Equal(EventReadStatus.Success, res.Status);
            Assert.Equal(res.Event.Value.OriginalEvent.EventId, _eventId1);
            Assert.True(res.Event.Value.OriginalEvent.IsJson);
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_last_event_in_stream_if_event_number_is_minus_one()
        {
            var res = _conn.ReadEventAsync("test-stream", -1, false).Result;

            Assert.Equal(EventReadStatus.Success, res.Status);
            Assert.Equal(res.Event.Value.OriginalEvent.EventId, _eventId1);
            Assert.Equal("test-stream", res.Stream);
            Assert.Equal(-1, res.EventNumber);
            Assert.NotEqual(DateTime.MinValue, res.Event.Value.OriginalEvent.Created);
            Assert.NotEqual(0, res.Event.Value.OriginalEvent.CreatedEpoch);
        }
    }
}
