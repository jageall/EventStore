using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Services;
using EventStore.Core.Tests.ClientAPI.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class read_all_events_backward_should : SpecificationWithMiniNode
    {
        private EventData[] _testEvents;

        protected override void When()
        {
            _conn.SetStreamMetadataAsync("$all", -1,
                                    StreamMetadata.Build().SetReadRole(SystemRoles.All),
                                    DefaultData.AdminCredentials)
            .Wait();

            var testEvents = Enumerable.Range(0, 20).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
            _conn.AppendToStreamAsync("stream", ExpectedVersion.EmptyStream, testEvents).Wait();
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._testEvents = testEvents;
            });
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void return_empty_slice_if_asked_to_read_from_start()
        {
            var read = _conn.ReadAllEventsBackwardAsync(Position.Start, 1, false).Result;
            Assert.True(read.IsEndOfStream);
            Assert.Equal(0, read.Events.Length);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void return_partial_slice_if_not_enough_events()
        {
            var read = _conn.ReadAllEventsBackwardAsync(Position.End, 30, false).Result;
            Assert.True(read.Events.Length < 30);
            Assert.True(EventDataComparer.Equal(_testEvents.Reverse().ToArray(),
                                                read.Events.Take(_testEvents.Length).Select(x => x.Event).ToArray()));
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void return_events_in_reversed_order_compared_to_written()
        {
            var read = _conn.ReadAllEventsBackwardAsync(Position.End, _testEvents.Length, false).Result;
            Assert.True(EventDataComparer.Equal(_testEvents.Reverse().ToArray(),
                                                read.Events.Select(x => x.Event).ToArray()));
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void be_able_to_read_all_one_by_one_until_end_of_stream()
        {
            var all = new List<RecordedEvent>();
            var position = Position.End;
            AllEventsSlice slice;

            while (!(slice = _conn.ReadAllEventsBackwardAsync(position, 1, false).Result).IsEndOfStream)
            {
                all.Add(slice.Events.Single().Event);
                position = slice.NextPosition;
            }

            Assert.True(EventDataComparer.Equal(_testEvents.Reverse().ToArray(), all.Take(_testEvents.Length).ToArray()));
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void be_able_to_read_events_slice_at_time()
        {
            var all = new List<RecordedEvent>();
            var position = Position.End;
            AllEventsSlice slice;

            while (!(slice = _conn.ReadAllEventsBackwardAsync(position, 5, false).Result).IsEndOfStream)
            {
                all.AddRange(slice.Events.Select(x => x.Event));
                position = slice.NextPosition;
            }

            Assert.True(EventDataComparer.Equal(_testEvents.Reverse().ToArray(), all.Take(_testEvents.Length).ToArray()));
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public async Task throw_when_got_int_max_value_as_maxcount()
        {
            await Assert.ThrowsAsync<ArgumentException>(
                () => _conn.ReadAllEventsBackwardAsync(Position.Start, int.MaxValue, resolveLinkTos: false));
        }
    }
}
