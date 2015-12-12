using System.Threading;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class subscribe_should : IClassFixture<MiniNodeFixture>
    {
        private const int Timeout = 10000;

        private MiniNode _node;

        public void SetFixture(MiniNodeFixture data)
        {
            _node = data.Node;
        }
        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.Create(node.TcpEndPoint);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void be_able_to_subscribe_to_non_existing_stream_and_then_catch_new_event()
        {
            const string stream = "subscribe_should_be_able_to_subscribe_to_non_existing_stream_and_then_catch_created_event";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var appeared = new CountdownEvent(1);
                var dropped = new CountdownEvent(1);

                using (store.SubscribeToStreamAsync(stream, false, (s, x) => appeared.Signal(), (s, r, e) => dropped.Signal()).Result)
                {
                    store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Wait();
                    Assert.True(appeared.Wait(Timeout), "Appeared countdown event timed out.");
                }
            }
        }

        [Fact][Trait("Category", "LongRunning")]
        public void allow_multiple_subscriptions_to_same_stream()
        {
            const string stream = "subscribe_should_allow_multiple_subscriptions_to_same_stream";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();
                var appeared = new CountdownEvent(2);
                var dropped = new CountdownEvent(2);

                using (store.SubscribeToStreamAsync(stream, false, (s, x) => appeared.Signal(), (s, r, e) => dropped.Signal()).Result)
                using (store.SubscribeToStreamAsync(stream, false, (s, x) => appeared.Signal(), (s, r, e) => dropped.Signal()).Result)
                {
                    store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Wait();
                    Assert.True(appeared.Wait(Timeout), "Appeared countdown event timed out.");
                }
            }
        }

        [Fact][Trait("Category", "LongRunning")]
        public void call_dropped_callback_after_unsubscribe_method_call()
        {
            const string stream = "subscribe_should_call_dropped_callback_after_unsubscribe_method_call";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var dropped = new CountdownEvent(1);
                using (var subscription = store.SubscribeToStreamAsync(stream, false, (s, x) => { }, (s, r, e) => dropped.Signal()).Result)
                {
                    subscription.Unsubscribe();
                }
                Assert.True(dropped.Wait(Timeout), "Dropped countdown event timed out.");
            }
        }

        [Fact][Trait("Category", "LongRunning")]
        public void catch_deleted_events_as_well()
        {
            const string stream = "subscribe_should_catch_created_and_deleted_events_as_well";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var appeared = new CountdownEvent(1);
                var dropped = new CountdownEvent(1);
                using (store.SubscribeToStreamAsync(stream, false, (s, x) => appeared.Signal(), (s, r, e) => dropped.Signal()).Result)
                {
                    store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true).Wait();
                    Assert.True(appeared.Wait(Timeout), "Appeared countdown event timed out.");
                }
            }
        }
    }
}
