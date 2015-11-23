using System;
using EventStore.ClientAPI.SystemData;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement
{
    public class enable_disable_user : TestWithUser
    {
        [Fact]
        public void disable_empty_username_throws()
        {
            Assert.Throws<ArgumentNullException>(() => _manager.DisableAsync("", new UserCredentials("admin", "changeit")).Wait());
        }

        [Fact]
        public void disable_null_username_throws()
        {
            Assert.Throws<ArgumentNullException>(() => _manager.DisableAsync(null, new UserCredentials("admin", "changeit")).Wait());
        }

        [Fact]
        public void enable_empty_username_throws()
        {
            Assert.Throws<ArgumentNullException>(() => _manager.EnableAsync("", new UserCredentials("admin", "changeit")).Wait());
        }

        [Fact]
        public void enable_null_username_throws()
        {
            Assert.Throws<ArgumentNullException>(() => _manager.EnableAsync(null, new UserCredentials("admin", "changeit")).Wait());
        }

        [Fact]
        public void can_enable_disable_user()
        {
            _manager.DisableAsync(_username, new UserCredentials("admin", "changeit")).Wait();

            Assert.Throws<AggregateException>(() => _manager.DisableAsync("foo", new UserCredentials(_username, "password")).Wait());

            _manager.EnableAsync(_username, new UserCredentials("admin", "changeit")).Wait();

            var c = _manager.GetCurrentUserAsync(new UserCredentials(_username, "password")).Result;
        }
    }
}