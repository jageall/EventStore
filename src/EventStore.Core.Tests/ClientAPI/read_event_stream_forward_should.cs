using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class read_event_stream_forward_should : IClassFixture<MiniNodeFixture>
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
        public async Task throw_if_count_le_zero()
        {
            const string stream = "read_event_stream_forward_should_throw_if_count_le_zero";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.ReadStreamEventsForwardAsync(stream, 0, 0, resolveLinkTos: false));
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public async Task throw_if_start_lt_zero()
        {
            const string stream = "read_event_stream_forward_should_throw_if_start_lt_zero";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.ReadStreamEventsForwardAsync(stream, -1, 1, resolveLinkTos: false));
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void notify_using_status_code_if_stream_not_found()
        {
            const string stream = "read_event_stream_forward_should_notify_using_status_code_if_stream_not_found";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var read = store.ReadStreamEventsForwardAsync(stream, 0, 1, resolveLinkTos: false);
                read.Wait();

                Assert.Equal(SliceReadStatus.StreamNotFound, read.Result.Status);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void notify_using_status_code_if_stream_was_deleted()
        {
            const string stream = "read_event_stream_forward_should_notify_using_status_code_if_stream_was_deleted";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var delete = store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                delete.Wait();

                var read = store.ReadStreamEventsForwardAsync(stream, 0, 1, resolveLinkTos: false);
                read.Wait();

                Assert.Equal(SliceReadStatus.StreamDeleted, read.Result.Status);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_no_events_when_called_on_empty_stream()
        {
            const string stream = "read_event_stream_forward_should_return_single_event_when_called_on_empty_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var read = store.ReadStreamEventsForwardAsync(stream, 0, 1, resolveLinkTos: false);
                read.Wait();

                Assert.Equal(0, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_empty_slice_when_called_on_non_existing_range()
        {
            const string stream = "read_event_stream_forward_should_return_empty_slice_when_called_on_non_existing_range";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var write10 = store.AppendToStreamAsync(stream, 
                                                        ExpectedVersion.EmptyStream, 
                                                        Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent((x + 1).ToString(CultureInfo.InvariantCulture))));
                write10.Wait();

                var read = store.ReadStreamEventsForwardAsync(stream, 11, 5, resolveLinkTos: false);
                read.Wait();

                Assert.Equal(0, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_partial_slice_if_not_enough_events_in_stream()
        {
            const string stream = "read_event_stream_forward_should_return_partial_slice_if_no_enough_events_in_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var write10 = store.AppendToStreamAsync(stream, 
                                                        ExpectedVersion.EmptyStream,
                                                        Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent((x + 1).ToString(CultureInfo.InvariantCulture))));
                write10.Wait();

                var read = store.ReadStreamEventsForwardAsync(stream, 9, 5, resolveLinkTos: false);
                read.Wait();

                Assert.Equal(1, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public async Task throw_when_got_int_max_value_as_maxcount()
        {
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                await Assert.ThrowsAsync<ArgumentException>(() => store.ReadStreamEventsForwardAsync("foo", StreamPosition.Start, int.MaxValue, resolveLinkTos: false));

            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void return_events_in_same_order_as_written()
        {
            const string stream = "read_event_stream_forward_should_return_events_in_same_order_as_written";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
                var write10 = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, testEvents);
                write10.Wait();

                var read = store.ReadStreamEventsForwardAsync(stream, StreamPosition.Start, testEvents.Length, resolveLinkTos: false);
                read.Wait();

                Assert.True(EventDataComparer.Equal(testEvents, read.Result.Events.Select(x => x.Event).ToArray()));
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void be_able_to_read_single_event_from_arbitrary_position()
        {
            const string stream = "read_event_stream_forward_should_be_able_to_read_from_arbitrary_position";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
                var write10 = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, testEvents);
                write10.Wait();

                var read = store.ReadStreamEventsForwardAsync(stream, 5, 1, resolveLinkTos: false);
                read.Wait();

                Assert.True(EventDataComparer.Equal(testEvents[5], read.Result.Events.Single().Event));
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void be_able_to_read_slice_from_arbitrary_position()
        {
            const string stream = "read_event_stream_forward_should_be_able_to_read_slice_from_arbitrary_position";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
                var write10 = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, testEvents);
                write10.Wait();

                var read = store.ReadStreamEventsForwardAsync(stream, 5, 2, resolveLinkTos: false);
                read.Wait();

                Assert.True(EventDataComparer.Equal(testEvents.Skip(5).Take(2).ToArray(), 
                                                    read.Result.Events.Select(x => x.Event).ToArray()));
            }
        }
    }
}
