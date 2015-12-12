using System;
using System.Linq;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class appending_to_implicitly_created_stream : SpecificationWithDirectoryPerTestFixture, IClassFixture<MiniNodeFixture>
    {
        private MiniNode _node;

        public appending_to_implicitly_created_stream(MiniNodeFixture data)
        {
            _node = data.Node;
        }

        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.Create(node.TcpEndPoint);
        }

        /*
         * sequence - events written so stream
         * 0em1 - event number 0 written with exp version -1 (minus 1)
         * 1any - event number 1 written with exp version any
         * S_0em1_1em1_E - START bucket, two events in bucket, END bucket
        */

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new StreamWriter(store, stream, -1);

                writer.Append(events).Then(events.First(), -1);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_4e4_0any_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_4e4_0any_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new StreamWriter(store, stream, -1);

                writer.Append(events).Then(events.First(), ExpectedVersion.Any);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new StreamWriter(store, stream, -1);

                writer.Append(events).Then(events.First(), 5);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length +1, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_wev()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_wev";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new StreamWriter(store, stream, -1);

                var first6 = writer.Append(events);
                var thrown = Assert.Throws<AggregateException>(() => first6.Then(events.First(), 6));
                Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new StreamWriter(store, stream, -1);

                var first6 = writer.Append(events);
                
                var thrown = Assert.Throws<AggregateException>(() => first6.Then(events.First(), 4));
                Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_0em1_0e0_non_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_0em1_0e0_non_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new StreamWriter(store, stream, -1);

                writer.Append(events).Then(events.First(), 0);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length + 1, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_0em1_0any_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_0em1_0any_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new StreamWriter(store, stream, -1);

                writer.Append(events).Then(events.First(), ExpectedVersion.Any);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_0em1_0em1_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_0em1_0em1_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new StreamWriter(store, stream, -1);

                writer.Append(events).Then(events.First(), -1);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_0em1_1e0_2e1_1any_1any_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_1any_1any_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 3).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new StreamWriter(store, stream, -1);

                writer.Append(events).Then(events[1], ExpectedVersion.Any).Then(events[1], ExpectedVersion.Any);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_S_0em1_1em1_E_S_0em1_E_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_0em1_E_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var append = store.AppendToStreamAsync(stream, -1, events);
                append.Wait();

                var app2 = store.AppendToStreamAsync(stream, -1, new[] { events.First() });
                app2.Wait();

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_S_0em1_1em1_E_S_0any_E_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_0any_E_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var append = store.AppendToStreamAsync(stream, -1, events);
                append.Wait();

                var app2 = store.AppendToStreamAsync(stream, ExpectedVersion.Any, new[] { events.First() });
                app2.Wait();

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_S_0em1_1em1_E_S_1e0_E_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_1e0_E_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var append = store.AppendToStreamAsync(stream, -1, events);
                append.Wait();

                var app2 = store.AppendToStreamAsync(stream, 0, new[] { events[1] });
                app2.Wait();

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_S_0em1_1em1_E_S_1any_E_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_1any_E_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var append = store.AppendToStreamAsync(stream, -1, events);
                append.Wait();

                var app2 = store.AppendToStreamAsync(stream, ExpectedVersion.Any, new[] { events[1] });
                app2.Wait();

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact, Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        public void sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail()
        {
            const string stream = "appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var append = store.AppendToStreamAsync(stream, -1, events);
                append.Wait();

                var app2 = store.AppendToStreamAsync(stream, -1, events.Concat(new[] { TestEvent.NewTestEvent(Guid.NewGuid()) }));
                var thrown = Assert.Throws<AggregateException>(() => app2.Wait());
                Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            }
        }
    }
}