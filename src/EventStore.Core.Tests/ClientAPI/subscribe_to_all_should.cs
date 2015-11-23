using System;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Services;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class subscribe_to_all_should : IUseFixture<SpecificationWithDirectory>, IDisposable
    {
        private const int Timeout = 10000;
        
        private MiniNode _node;
        private IEventStoreConnection _conn;

        public void SetFixture(SpecificationWithDirectory data)
        {
            _node = new MiniNode(data.PathName, skipInitializeStandardUsersCheck: false);
            _node.Start();

            _conn = BuildConnection(_node);
            _conn.ConnectAsync().Wait();
            _conn.SetStreamMetadataAsync("$all", -1,
                                    StreamMetadata.Build().SetReadRole(SystemRoles.All),
                                    new UserCredentials(SystemUsers.Admin, SystemUsers.DefaultAdminPassword)).Wait();
        }

        public void Dispose()
        {
            _conn.Close();
            _node.Shutdown();
        }

        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.Create(node.TcpEndPoint);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void allow_multiple_subscriptions()
        {
            const string stream = "subscribe_to_all_should_allow_multiple_subscriptions";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();
                var appeared = new CountdownEvent(2);
                var dropped = new CountdownEvent(2);

                using (store.SubscribeToAllAsync(false, (s, x) => appeared.Signal(), (s, r, e) => dropped.Signal()).Result)
                using (store.SubscribeToAllAsync(false, (s, x) => appeared.Signal(), (s, r, e) => dropped.Signal()).Result)
                {
                    var create = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent());
                    Assert.True(create.Wait(Timeout), "StreamCreateAsync timed out.");

                    Assert.True(appeared.Wait(Timeout), "Appeared countdown event timed out.");
                }
            }
        }

        [Fact][Trait("Category", "LongRunning")]
        public void catch_deleted_events_as_well()
        {
            const string stream = "subscribe_to_all_should_catch_created_and_deleted_events_as_well";
            using (var store = TestConnection.Create(_node.TcpEndPoint))
            {
                store.ConnectAsync().Wait();
                var appeared = new CountdownEvent(1);
                var dropped = new CountdownEvent(1);

                using (store.SubscribeToAllAsync(false, (s, x) => appeared.Signal(), (s, r, e) => dropped.Signal()).Result)
                {
                    var delete = store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                    Assert.True(delete.Wait(Timeout), "DeleteStreamAsync timed out.");

                    Assert.True(appeared.Wait(Timeout), "Appeared countdown event didn't fire in time.");
                }
            }
        }
    }
}
