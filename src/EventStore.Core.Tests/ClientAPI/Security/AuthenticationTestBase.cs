using System;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services;
using EventStore.Core.Services.UserManagement;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security
{
    public abstract class AuthenticationTestBase : IUseFixture<AuthenticationTestBase.Fixture>
    {
        public class Fixture : MiniNodeFixture
        {
            public IEventStoreConnection Connection;
            private Action<Func<MiniNode, IEventStoreConnection>, Action> _initialize;

            public Fixture()
            {
                _initialize = (setupConnection, additionalSetup) =>
                {
                    _initialize = (_, __) => { };
                    var userCreateEvent1 = new ManualResetEventSlim();
                    Node.Node.MainQueue.Publish(
                        new UserManagementMessage.Create(
                            new CallbackEnvelope(
                                m =>
                                {
                                    Assert.True(m is UserManagementMessage.UpdateResult);
                                    var msg = (UserManagementMessage.UpdateResult) m;
                                    Assert.True(msg.Success);

                                    userCreateEvent1.Set();
                                }),
                            SystemAccount.Principal,
                            "user1",
                            "Test User 1",
                            new string[0],
                            "pa$$1"));

                    var userCreateEvent2 = new ManualResetEventSlim();
                    Node.Node.MainQueue.Publish(
                        new UserManagementMessage.Create(
                            new CallbackEnvelope(
                                m =>
                                {
                                    Assert.True(m is UserManagementMessage.UpdateResult);
                                    var msg = (UserManagementMessage.UpdateResult) m;
                                    Assert.True(msg.Success);

                                    userCreateEvent2.Set();
                                }),
                            SystemAccount.Principal,
                            "user2",
                            "Test User 2",
                            new string[0],
                            "pa$$2"));

                    var adminCreateEvent2 = new ManualResetEventSlim();
                    Node.Node.MainQueue.Publish(
                        new UserManagementMessage.Create(
                            new CallbackEnvelope(
                                m =>
                                {
                                    Assert.True(m is UserManagementMessage.UpdateResult);
                                    var msg = (UserManagementMessage.UpdateResult) m;
                                    Assert.True(msg.Success);

                                    adminCreateEvent2.Set();
                                }),
                            SystemAccount.Principal,
                            "adm",
                            "Administrator User",
                            new[] {SystemRoles.Admins},
                            "admpa$$"));

                    Assert.True(userCreateEvent1.Wait(10000), "User 1 creation failed");
                    Assert.True(userCreateEvent2.Wait(10000), "User 2 creation failed");
                    Assert.True(adminCreateEvent2.Wait(10000), "Administrator User creation failed");

                    Connection = setupConnection(Node);
                    Connection.ConnectAsync().Wait();

                    Connection.SetStreamMetadataAsync("noacl-stream", ExpectedVersion.NoStream, StreamMetadata.Build())
                        .Wait();
                    Connection.SetStreamMetadataAsync(
                        "read-stream",
                        ExpectedVersion.NoStream,
                        StreamMetadata.Build().SetReadRole("user1")).Wait();
                    Connection.SetStreamMetadataAsync(
                        "write-stream",
                        ExpectedVersion.NoStream,
                        StreamMetadata.Build().SetWriteRole("user1")).Wait();
                    Connection.SetStreamMetadataAsync(
                        "metaread-stream",
                        ExpectedVersion.NoStream,
                        StreamMetadata.Build().SetMetadataReadRole("user1")).Wait();
                    Connection.SetStreamMetadataAsync(
                        "metawrite-stream",
                        ExpectedVersion.NoStream,
                        StreamMetadata.Build().SetMetadataWriteRole("user1")).Wait();

                    Connection.SetStreamMetadataAsync(
                        "$all",
                        ExpectedVersion.Any,
                        StreamMetadata.Build().SetReadRole("user1"),
                        new UserCredentials("adm", "admpa$$")).Wait();

                    Connection.SetStreamMetadataAsync(
                        "$system-acl",
                        ExpectedVersion.NoStream,
                        StreamMetadata.Build()
                            .SetReadRole("user1")
                            .SetWriteRole("user1")
                            .SetMetadataReadRole("user1")
                            .SetMetadataWriteRole("user1"),
                        new UserCredentials("adm", "admpa$$")).Wait();
                    Connection.SetStreamMetadataAsync(
                        "$system-adm",
                        ExpectedVersion.NoStream,
                        StreamMetadata.Build()
                            .SetReadRole(SystemRoles.Admins)
                            .SetWriteRole(SystemRoles.Admins)
                            .SetMetadataReadRole(SystemRoles.Admins)
                            .SetMetadataWriteRole(SystemRoles.Admins),
                        new UserCredentials("adm", "admpa$$")).Wait();

                    Connection.SetStreamMetadataAsync(
                        "normal-all",
                        ExpectedVersion.NoStream,
                        StreamMetadata.Build()
                            .SetReadRole(SystemRoles.All)
                            .SetWriteRole(SystemRoles.All)
                            .SetMetadataReadRole(SystemRoles.All)
                            .SetMetadataWriteRole(SystemRoles.All)).Wait();
                    Connection.SetStreamMetadataAsync(
                        "$system-all",
                        ExpectedVersion.NoStream,
                        StreamMetadata.Build()
                            .SetReadRole(SystemRoles.All)
                            .SetWriteRole(SystemRoles.All)
                            .SetMetadataReadRole(SystemRoles.All)
                            .SetMetadataWriteRole(SystemRoles.All),
                        new UserCredentials("adm", "admpa$$")).Wait();
                    additionalSetup();
                };
            }

            public override void Dispose()
            {
                Connection.Close();
                base.Dispose();
            }

            public void Initialize(Func<MiniNode, IEventStoreConnection> setupConnection, Action additionalSetup)
            {
                _initialize(setupConnection, additionalSetup);
            }
        }
        private readonly UserCredentials _userCredentials;
        protected IEventStoreConnection Connection{get { return _fixture.Connection; }}
        private Fixture _fixture;

        protected AuthenticationTestBase(UserCredentials userCredentials = null)
        {
            _userCredentials = userCredentials;
        }

        public virtual IEventStoreConnection SetupConnection(MiniNode node)
        {
            return TestConnection.Create(node.TcpEndPoint, TcpType.Normal, _userCredentials);
        }

        public virtual void SetFixture(Fixture fixture)
        {
            _fixture = fixture;
            fixture.Initialize(SetupConnection, AdditionalFixtureSetup);
        }

        protected virtual void AdditionalFixtureSetup(){}

        protected void ReadEvent(string streamId, string login, string password)
        {
            Connection.ReadEventAsync(streamId, -1, false,
                                 login == null && password == null ? null : new UserCredentials(login, password))
            .Wait();
        }

        protected void ReadStreamForward(string streamId, string login, string password)
        {
            Connection.ReadStreamEventsForwardAsync(streamId, 0, 1, false,
                                               login == null && password == null ? null : new UserCredentials(login, password))
            .Wait();
        }

        protected void ReadStreamBackward(string streamId, string login, string password)
        {
            Connection.ReadStreamEventsBackwardAsync(streamId, 0, 1, false,
                                                login == null && password == null ? null : new UserCredentials(login, password))
            .Wait();
        }

        protected void WriteStream(string streamId, string login, string password)
        {
            Connection.AppendToStreamAsync(streamId, ExpectedVersion.Any, CreateEvents(),
                                      login == null && password == null ? null : new UserCredentials(login, password))
            .Wait();
        }

        protected EventStoreTransaction TransStart(string streamId, string login, string password)
        {
            return Connection.StartTransactionAsync(streamId, ExpectedVersion.Any,
                                                login == null && password == null ? null : new UserCredentials(login, password))
            .Result;
        }

        protected void ReadAllForward(string login, string password)
        {
            Connection.ReadAllEventsForwardAsync(Position.Start, 1, false,
                                            login == null && password == null ? null : new UserCredentials(login, password))
            .Wait();
        }

        protected void ReadAllBackward(string login, string password)
        {
            Connection.ReadAllEventsBackwardAsync(Position.End, 1, false,
                                             login == null && password == null ? null : new UserCredentials(login, password))
            .Wait();
        }

        protected void ReadMeta(string streamId, string login, string password)
        {
            Connection.GetStreamMetadataAsRawBytesAsync(streamId, login == null && password == null ? null : new UserCredentials(login, password)).Wait();
        }

        protected void WriteMeta(string streamId, string login, string password, string metawriteRole)
        {
            Connection.SetStreamMetadataAsync(streamId, ExpectedVersion.Any,
                                         metawriteRole == null
                                            ? StreamMetadata.Build()
                                            : StreamMetadata.Build().SetReadRole(metawriteRole)
                                                                    .SetWriteRole(metawriteRole)
                                                                    .SetMetadataReadRole(metawriteRole)
                                                                    .SetMetadataWriteRole(metawriteRole),
                                         login == null && password == null ? null : new UserCredentials(login, password))
            .Wait();
        }

        protected void SubscribeToStream(string streamId, string login, string password)
        {
            using (Connection.SubscribeToStreamAsync(streamId, false, (x, y) => { }, (x, y, z) => { },
                                                login == null && password == null ? null : new UserCredentials(login, password)).Result)
            {
            }
        }

        protected void SubscribeToAll(string login, string password)
        {
            using (Connection.SubscribeToAllAsync(false, (x, y) => { }, (x, y, z) => { },
                                             login == null && password == null ? null : new UserCredentials(login, password)).Result)
            {
            }
        }

        protected string CreateStreamWithMeta(StreamMetadata metadata, string stream = null, string streamPrefix = null)
        {
            stream = (streamPrefix ?? "") + (stream ?? Guid.NewGuid().ToString("N"));
            Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
                                         metadata, new UserCredentials("adm", "admpa$$")).Wait();
            return stream;
        }

        protected void DeleteStream(string streamId, string login, string password)
        {
            Connection.DeleteStreamAsync(streamId, ExpectedVersion.Any, true,
                                    login == null && password == null ? null : new UserCredentials(login, password)).Wait();
        }

        protected void Expect<T>(Action action) where T : Exception
        {
            var inner = Assert.Throws<AggregateException>(() => action());
            Assert.IsType<T>(inner.InnerException);
        }

        protected void ExpectNoException(Action action)
        {
            Assert.DoesNotThrow(() => action());
        }

        protected EventData[] CreateEvents()
        {
            return new[] { new EventData(Guid.NewGuid(), "some-type", false, new byte[] { 1, 2, 3 }, null) };
        }
    }
}