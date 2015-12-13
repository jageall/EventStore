using System.Collections.Generic;
using EventStore.ClientAPI;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class read_all_events_forward_with_linkto_passed_max_count : SpecificationWithLinkToToMaxCountDeletedEvents
    {
        private StreamEventsSlice _read;

        protected override void When()
        {
            _read = _conn.ReadStreamEventsForwardAsync(LinkedStreamName, 0, 1, true).Result;
        }

        [Fact]
        public void one_event_is_read()
        {
            Assert.Equal(1, _read.Events.Length);
        }

        public read_all_events_forward_with_linkto_passed_max_count(SpecificationFixture fixture) : base(fixture)
        {
        }
    }
}