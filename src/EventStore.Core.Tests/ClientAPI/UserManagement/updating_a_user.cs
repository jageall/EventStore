using System;
using System.Net;
using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using EventStore.ClientAPI.UserManagement;
using Xunit;
using HttpStatusCode = EventStore.ClientAPI.Transport.Http.HttpStatusCode;

namespace EventStore.Core.Tests.ClientAPI.UserManagement
{
    public class updating_a_user : TestWithNode
    {
        [Fact]
        public async  Task updating_a_user_with_null_username_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.UpdateUserAsync(null, "greg", new[] { "foo", "bar" }, new UserCredentials("admin", "changeit")));
        }

        [Fact]
        public async Task updating_a_user_with_empty_username_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.UpdateUserAsync("", "greg", new[] { "foo", "bar" }, new UserCredentials("admin", "changeit")));
        }

        [Fact]
        public async Task updating_a_user_with_null_name_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.UpdateUserAsync("greg", null, new[] { "foo", "bar" }, new UserCredentials("admin", "changeit")));
        }

        [Fact]
        public async Task updating_a_user_with_empty_name_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.UpdateUserAsync("greg", "", new[] { "foo", "bar" }, new UserCredentials("admin", "changeit")));
        }

        [Fact]
        public void updating_non_existing_user_throws()
        {
            Assert.Throws<AggregateException>(() => _manager.UpdateUserAsync(Guid.NewGuid().ToString(), "bar", new []{"foo"}, new UserCredentials("admin", "changeit")).Wait());
        }

        [Fact]
        public void updating_a_user_with_parameters_can_be_read()
        {
            UserDetails d = null;
            _manager.CreateUserAsync("ouro", "ourofull", new[] {"foo", "bar"}, "password",
                new UserCredentials("admin", "changeit")).Wait();
            _manager.UpdateUserAsync("ouro", "something", new[] {"bar", "baz"}, new UserCredentials("admin", "changeit"))
                .Wait();
            d = _manager.GetUserAsync("ouro", new UserCredentials("admin", "changeit")).Result;
            
            Assert.Equal("ouro", d.LoginName);
            Assert.Equal("something", d.FullName);
            Assert.Equal("bar", d.Groups[0]);
            Assert.Equal("baz", d.Groups[1]);
        }
    }
}
