using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Tests.ClientAPI.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Embedded.Security
{
    public class authorized_default_credentials_security : EventStore.Core.Tests.ClientAPI.Security.authorized_default_credentials_security
    {
        public authorized_default_credentials_security(Fixture fixture) : base(fixture)
        {
            
        }
        public override EventStore.ClientAPI.IEventStoreConnection SetupConnection(Tests.Helpers.MiniNode node)
        {
            return EmbeddedTestConnection.To(node, DefaultData.AdminCredentials);
        }
    }
}