using System;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.UserManagement;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement
{
    public class TestWithNode : SpecificationWithDirectoryPerTestFixture
    {
        protected MiniNode _node;
        protected UsersManager _manager;

        public TestWithNode()
        {
            _node = new MiniNode(PathName);
            _node.Start();
            _manager = new UsersManager(new NoopLogger(), _node.ExtHttpEndPoint, TimeSpan.FromSeconds(5));
        }

        public override void Dispose()
        {
            _node.Shutdown();
            base.Dispose();
        }


        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.Create(node.TcpEndPoint);
        }

    }
}