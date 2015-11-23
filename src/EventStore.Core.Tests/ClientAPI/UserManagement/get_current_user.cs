using EventStore.ClientAPI.SystemData;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement
{
    public class get_current_user : TestWithNode
    {
        [Fact]
        public void returns_the_current_user()
        {
            var x = _manager.GetCurrentUserAsync(new UserCredentials("admin", "changeit")).Result;
            Assert.Equal("admin", x.LoginName);
            Assert.Equal("Event Store Administrator", x.FullName);
        }
    }
}