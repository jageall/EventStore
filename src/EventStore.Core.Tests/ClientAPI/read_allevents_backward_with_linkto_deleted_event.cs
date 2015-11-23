using EventStore.ClientAPI;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class read_event_of_linkto_to_deleted_event : SpecificationWithLinkToToDeletedEvents
    {
        private EventReadResult _read;
        protected override void When()
        {
            var read = _conn.ReadEventAsync(LinkedStreamName, 0,true).Result;
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._read = read;
            });
        }

        [Fact]
        public void the_linked_event_is_returned()
        {
            Assert.NotNull(_read.Event.Value.Link);
        }

        [Fact]
        public void the_deleted_event_is_not_resolved()
        {
            Assert.Null(_read.Event.Value.Event);
        }

        [Fact]
        public void the_status_is_success()
        {
            Assert.Equal(EventReadStatus.Success, _read.Status);
        }
    }

    public class read_allevents_backward_with_linkto_deleted_event : SpecificationWithLinkToToDeletedEvents
    {
        private StreamEventsSlice _read;
        protected override void When()
        {
            var read = _conn.ReadStreamEventsBackwardAsync(LinkedStreamName, 0, 1, true, null).Result;
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._read = read;
            });
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void one_event_is_read()
        {
            Assert.Equal(1, _read.Events.Length);
        }

        [Fact]
        public void the_linked_event_is_not_resolved()
        {
            Assert.Null(_read.Events[0].Event);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_link_event_is_included()
        {
            Assert.NotNull(_read.Events[0].OriginalEvent);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_event_is_not_resolved()
        {
            Assert.False(_read.Events[0].IsResolved);
        }
    }
}