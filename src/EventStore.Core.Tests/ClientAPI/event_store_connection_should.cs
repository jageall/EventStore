using System;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace EventStore.Core.Tests.ClientAPI
{
    public class event_store_connection_should: IClassFixture<MiniNodeFixture>
    {
        private MiniNode _node;

        public event_store_connection_should(MiniNodeFixture data)
        {
            _node = data.Node;
        }

        [Theory, InlineData(TcpType.Ssl), InlineData(TcpType.Normal)]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void not_throw_on_close_if_connect_was_not_called(TcpType tcpType)
        {
            var connection = TestConnection.To(_node, tcpType);
            var ex = Record.Exception(() => connection.Close());
            Assert.Null(ex);
        }

        [Theory, InlineData(TcpType.Ssl), InlineData(TcpType.Normal)]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void not_throw_on_close_if_called_multiple_times(TcpType tcpType)
        {
            var connection = TestConnection.To(_node, tcpType);
            connection.ConnectAsync().Wait();
            connection.Close();
            var ex = Record.Exception(() => connection.Close());
            Assert.Null(ex);
        }
/*
//TODO WEIRD TEST GFY
        [Fact]
        [Trait("Category", "Network")]
        public void throw_on_connect_called_more_than_once()
        {
            var connection = TestConnection.To(Node, _tcpType);
            Assert.DoesNotThrow(() => connection.ConnectAsync().Wait());

            Assert.True(() => connection.ConnectAsync().Wait(),
                        Throws.Exception.InstanceOf<AggregateException>().With.InnerException.InstanceOf<InvalidOperationException>());
        }

        [Fact]
        [Trait("Category", "Network")]
        public void throw_on_connect_called_after_close()
        {
            var connection = TestConnection.To(Node, _tcpType);
            connection.ConnectAsync().Wait();
            connection.Close();

            Assert.True(() => connection.ConnectAsync().Wait(),
                        Throws.Exception.InstanceOf<AggregateException>().With.InnerException.InstanceOf<InvalidOperationException>());
        }
*/
        [Theory, InlineData(TcpType.Ssl), InlineData(TcpType.Normal)]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void throw_invalid_operation_on_every_api_call_if_connect_was_not_called(TcpType tcpType)
        {
            var connection = TestConnection.To(_node, tcpType);

            const string s = "stream";
            var events = new[] { TestEvent.NewTestEvent() };
            var thrown = Assert.Throws<AggregateException>(() => connection.DeleteStreamAsync(s, 0).Wait());
            Assert.IsType<InvalidOperationException>(thrown.InnerException);

            thrown = Assert.Throws<AggregateException>(() => connection.AppendToStreamAsync(s, 0, events).Wait());
            Assert.IsType<InvalidOperationException>(thrown.InnerException);

            thrown = Assert.Throws<AggregateException>(() => connection.ReadStreamEventsForwardAsync(s, 0, 1, resolveLinkTos: false).Wait()); 
            Assert.IsType<InvalidOperationException>(thrown.InnerException);

            thrown = Assert.Throws<AggregateException>(() => connection.ReadStreamEventsBackwardAsync(s, 0, 1, resolveLinkTos: false).Wait());
            Assert.IsType<InvalidOperationException>(thrown.InnerException);

            thrown = Assert.Throws<AggregateException>(() => connection.ReadAllEventsForwardAsync(Position.Start, 1, false).Wait());
            Assert.IsType<InvalidOperationException>(thrown.InnerException);

            thrown = Assert.Throws<AggregateException>(() => connection.ReadAllEventsBackwardAsync(Position.End, 1, false).Wait());
            Assert.IsType<InvalidOperationException>(thrown.InnerException);

            thrown = Assert.Throws<AggregateException>(() => connection.StartTransactionAsync(s, 0).Wait());
            Assert.IsType<InvalidOperationException>(thrown.InnerException);

            thrown = Assert.Throws<AggregateException>(() => connection.SubscribeToStreamAsync(s, false, (_, __) => { }, (_, __, ___) => { }).Wait());
            Assert.IsType<InvalidOperationException>(thrown.InnerException);

            thrown = Assert.Throws<AggregateException>(() => connection.SubscribeToAllAsync(false, (_, __) => { }, (_, __, ___) => { }).Wait());
            Assert.IsType<InvalidOperationException>(thrown.InnerException);
        }
    }
}
