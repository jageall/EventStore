using System;
using System.Linq;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class isjson_flag_on_event : IClassFixture<MiniNodeFixture>
    {
        private MiniNode _node;

        public isjson_flag_on_event(MiniNodeFixture data)
        {
            _node = data.Node;
        }
        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.To(node, TcpType.Normal);
        }

        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        [Fact]
        public void should_be_preserved_with_all_possible_write_and_read_methods()
        {
            const string stream = "should_be_preserved_with_all_possible_write_methods";
            using (var connection = BuildConnection(_node))
            {
                connection.ConnectAsync().Wait();

                connection.AppendToStreamAsync(
                    stream,
                    ExpectedVersion.Any,
                    new EventData(Guid.NewGuid(), "some-type", true, Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}"), null),
                    new EventData(Guid.NewGuid(), "some-type", true, null, Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}")),
                    new EventData(Guid.NewGuid(), "some-type", true, Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}"), Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}")))
                .Wait();

                using (var transaction = connection.StartTransactionAsync(stream, ExpectedVersion.Any).Result)
                {
                    transaction.WriteAsync(
                        new EventData(Guid.NewGuid(), "some-type", true,
                            Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}"), null),
                        new EventData(Guid.NewGuid(), "some-type", true, null,
                            Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}")),
                        new EventData(Guid.NewGuid(), "some-type", true,
                            Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}"),
                            Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}"))).Wait();
                    transaction.CommitAsync().Wait();
                }

                var done = new ManualResetEventSlim();
                _node.Node.MainQueue.Publish(new ClientMessage.ReadStreamEventsForward(
                    Guid.NewGuid(), Guid.NewGuid(), new CallbackEnvelope(message =>
                    {
                        Assert.IsType<ClientMessage.ReadStreamEventsForwardCompleted>(message);
                        var msg = (ClientMessage.ReadStreamEventsForwardCompleted) message;
                        Assert.Equal(Data.ReadStreamResult.Success, msg.Result);
                        Assert.Equal(6, msg.Events.Length);
                        Assert.True(msg.Events.All(x => (x.OriginalEvent.Flags & PrepareFlags.IsJson) != 0));

                        done.Set();
                    }), stream, 0, 100, false, false, null, null));
                Assert.True(done.Wait(10000), "Read was not completed in time.");
            }
        }
    }
}
