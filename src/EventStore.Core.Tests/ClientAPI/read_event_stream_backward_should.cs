using System;
using System.Linq;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class read_event_stream_backward_should : IUseFixture<MiniNodeFixture>
    {
        private MiniNode _node;

        public void SetFixture(MiniNodeFixture data)
        {
            _node = data.Node;
        }

        virtual protected IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.Create(node.TcpEndPoint);
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void throw_if_count_le_zero()
        {
            const string stream = "read_event_stream_backward_should_throw_if_count_le_zero";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                Assert.Throws<ArgumentOutOfRangeException>(() => store.ReadStreamEventsBackwardAsync(stream, 0, 0, resolveLinkTos: false));
            }
        }


        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void notify_using_status_code_if_stream_not_found()
        {
            const string stream = "read_event_stream_backward_should_notify_using_status_code_if_stream_not_found";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var read = store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);

                Assert.Equal(SliceReadStatus.StreamNotFound, read.Result.Status);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void notify_using_status_code_if_stream_was_deleted()
        {
            const string stream = "read_event_stream_backward_should_notify_using_status_code_if_stream_was_deleted";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var delete = store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                Assert.DoesNotThrow(delete.Wait);

                var read = store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);

                Assert.Equal(SliceReadStatus.StreamDeleted, read.Result.Status);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_no_events_when_called_on_empty_stream()
        {
            const string stream = "read_event_stream_backward_should_return_single_event_when_called_on_empty_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var read = store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);

                Assert.Equal(0, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_partial_slice_if_no_enough_events_in_stream()
        {
            const string stream = "read_event_stream_backward_should_return_partial_slice_if_no_enough_events_in_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent((x + 1).ToString())).ToArray();
                var write10 = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, testEvents);
                Assert.DoesNotThrow(write10.Wait);

                var read = store.ReadStreamEventsBackwardAsync(stream, 1, 5, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);

                Assert.Equal(2, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_events_reversed_compared_to_written()
        {
            const string stream = "read_event_stream_backward_should_return_events_reversed_compared_to_written";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent((x + 1).ToString())).ToArray();
                var write10 = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, testEvents);
                Assert.DoesNotThrow(write10.Wait);

                var read = store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, testEvents.Length, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);

                Assert.True(EventDataComparer.Equal(testEvents.Reverse().ToArray(), read.Result.Events.Select(x => x.Event).ToArray()));
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void be_able_to_read_single_event_from_arbitrary_position()
        {
            const string stream = "read_event_stream_backward_should_be_able_to_read_single_event_from_arbitrary_position";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
                var write10 = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, testEvents);
                Assert.DoesNotThrow(write10.Wait);

                var read = store.ReadStreamEventsBackwardAsync(stream, 7, 1, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);

                Assert.True(EventDataComparer.Equal(testEvents[7], read.Result.Events.Single().Event));
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void be_able_to_read_first_event()
        {
            const string stream = "read_event_stream_backward_should_be_able_to_read_first_event";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent((x + 1).ToString())).ToArray();
                var write10 = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, testEvents);
                Assert.DoesNotThrow(write10.Wait);

                var read = store.ReadStreamEventsBackwardAsync(stream, StreamPosition.Start, 1, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);

                Assert.Equal(1, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void be_able_to_read_last_event()
        {
            const string stream = "read_event_stream_backward_should_be_able_to_read_last_event";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
                var write10 = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, testEvents);
                Assert.DoesNotThrow(write10.Wait);

                var read = store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);

                Assert.True(EventDataComparer.Equal(testEvents.Last(), read.Result.Events.Single().Event));
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void be_able_to_read_slice_from_arbitrary_position()
        {
            const string stream = "read_event_stream_backward_should_be_able_to_read_slice_from_arbitrary_position";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
                var write10 = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, testEvents);
                Assert.DoesNotThrow(write10.Wait);

                var read = store.ReadStreamEventsBackwardAsync(stream, 3, 2, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);

                Assert.True(EventDataComparer.Equal(testEvents.Skip(2).Take(2).Reverse().ToArray(), 
                                                     read.Result.Events.Select(x => x.Event).ToArray()));
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void throw_when_got_int_max_value_as_maxcount()
        {
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                Assert.Throws<ArgumentException>(() => store.ReadStreamEventsBackwardAsync("foo", StreamPosition.Start, int.MaxValue, resolveLinkTos: false));

            }
        }
    }
}
