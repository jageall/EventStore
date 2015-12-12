using System;
using System.Linq;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class appending_to_implicitly_created_stream_using_transaction : SpecificationWithDirectoryPerTestFixture, IClassFixture<MiniNodeFixture>
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

        /*
         * sequence - events written so stream
         * 0em1 - event number 0 written with exp version -1 (minus 1)
         * 1any - event number 1 written with exp version any
         * S_0em1_1em1_E - START bucket, two events in bucket, END bucket
        */

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(5, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                Assert.Equal(0, writer.StartTransaction(-1).Write(events.First()).Commit().NextExpectedVersion);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_5e4_0any_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0any_idempotent";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(5, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                Assert.Equal(0, writer.StartTransaction(ExpectedVersion.Any).Write(events.First()).Commit().NextExpectedVersion);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(5, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                Assert.Equal(6, writer.StartTransaction(5).Write(events.First()).Commit().NextExpectedVersion);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length + 1, total);
            }
        }

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_wev()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_wev";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(5, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                
                var thrown = Assert.Throws<AggregateException>(() => writer.StartTransaction(6).Write(events.First()).Commit());
                Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(5, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                
                var thrown = Assert.Throws<AggregateException>(() => writer.StartTransaction(4).Write(events.First()).Commit());
                Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_0em1_0e0_non_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_0em1_0e0_non_idempotent";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(0, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                Assert.Equal(1, writer.StartTransaction(0).Write(events.First()).Commit().NextExpectedVersion);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length + 1, total);
            }
        }

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_0em1_0any_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_0em1_0any_idempotent";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(0, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                Assert.Equal(0, writer.StartTransaction(ExpectedVersion.Any).Write(events.First()).Commit().NextExpectedVersion);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_0em1_0em1_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_0em1_0em1_idempotent";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(0, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                Assert.Equal(0, writer.StartTransaction(-1).Write(events.First()).Commit().NextExpectedVersion);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_0em1_1e0_2e1_1any_1any_idempotent()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_1any_1any_idempotent";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 3).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(2, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                Assert.Equal(1, writer.StartTransaction(ExpectedVersion.Any).Write(events[1]).Write(events[1]).Commit().NextExpectedVersion);

                var total = EventsStream.Count(store, stream);
                Assert.Equal(events.Length, total);
            }
        }

        [Fact]
        [Trait("Category", "Network"), Trait("Category", "LongRunning")]
        public void sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail()
        {
            const string stream = "appending_to_implicitly_created_stream_using_transaction_sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
                var writer = new TransactionalWriter(store, stream);

                Assert.Equal(1, writer.StartTransaction(-1).Write(events).Commit().NextExpectedVersion);
                var thrown = Assert.Throws<AggregateException>(() => writer.StartTransaction(-1)
                                        .Write(events.Concat(new[] { TestEvent.NewTestEvent(Guid.NewGuid()) }).ToArray())
                                        .Commit());
                Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            }
        }
    }
}
