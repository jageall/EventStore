using EventStore.ClientAPI;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class read_all_events_forward_with_linkto_to_deleted_event : SpecificationWithLinkToToDeletedEvents
    {
        private StreamEventsSlice _read;
        protected override void When()
        {
            var read = _conn.ReadStreamEventsForwardAsync(LinkedStreamName, 0, 1, true, null).Result;
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
        [Trait("Category", "LongRunning")]
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

        public read_all_events_forward_with_linkto_to_deleted_event(SpecificationFixture fixture) : base(fixture)
        {
        }
    }
}