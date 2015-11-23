using System;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class when_committing_empty_transaction : SpecificationWithDirectory
    {
        private MiniNode _node;
        private IEventStoreConnection _connection;
        private EventData _firstEvent;

        public  when_committing_empty_transaction()
        {
            _node = new MiniNode(PathName);
            _node.Start();

            _firstEvent = TestEvent.NewTestEvent();

            _connection = BuildConnection(_node);
            _connection.ConnectAsync().Wait();

            Assert.Equal(2, _connection.AppendToStreamAsync("test-stream",
                                                          ExpectedVersion.NoStream,
                                                          _firstEvent,
                                                          TestEvent.NewTestEvent(),
                                                          TestEvent.NewTestEvent()).Result.NextExpectedVersion);

            using (var transaction = _connection.StartTransactionAsync("test-stream", 2).Result)
            {
                Assert.Equal(2, transaction.CommitAsync().Result.NextExpectedVersion);
            }
        }

        public override void Dispose()
        {
            _connection.Close();
            _node.Shutdown();
            base.Dispose();
        }

        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.Create(node.TcpEndPoint);
        }

        [Fact]
        public void following_append_with_correct_expected_version_are_commited_correctly()
        {
            Assert.Equal(4, _connection.AppendToStreamAsync("test-stream", 2, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);

            var res = _connection.ReadStreamEventsForwardAsync("test-stream", 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(5, res.Events.Length);
            for (int i=0; i<5; ++i)
            {
                Assert.Equal(i, res.Events[i].OriginalEventNumber);
            }
        }

        [Fact]
        public void following_append_with_expected_version_any_are_commited_correctly()
        {
            Assert.Equal(4, _connection.AppendToStreamAsync("test-stream", ExpectedVersion.Any, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);

            var res = _connection.ReadStreamEventsForwardAsync("test-stream", 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(5, res.Events.Length);
            for (int i = 0; i < 5; ++i)
            {
                Assert.Equal(i, res.Events[i].OriginalEventNumber);
            }
        }

        [Fact]
        public void committing_first_event_with_expected_version_no_stream_is_idempotent()
        {
            Assert.Equal(0, _connection.AppendToStreamAsync("test-stream", ExpectedVersion.NoStream, _firstEvent).Result.NextExpectedVersion);

            var res = _connection.ReadStreamEventsForwardAsync("test-stream", 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(3, res.Events.Length);
            for (int i = 0; i < 3; ++i)
            {
                Assert.Equal(i, res.Events[i].OriginalEventNumber);
            }
        }

        [Fact]
        public void trying_to_append_new_events_with_expected_version_no_stream_fails()
        {
            var thrown = Assert.Throws<AggregateException>(() => _connection.AppendToStreamAsync("test-stream", ExpectedVersion.NoStream, TestEvent.NewTestEvent()).Result);
            Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
        }
    }
}
