using System;
using System.Threading.Tasks;
using EventStore.ClientAPI.SystemData;
using EventStore.ClientAPI.UserManagement;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement
{
    public class creating_a_user : TestWithNode 
    {
        [Fact]
        public async Task creating_a_user_with_null_username_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.CreateUserAsync(null, "greg", new[] {"foo", "bar"}, "foofoofoo"));
        }

        [Fact]
        public async Task creating_a_user_with_empty_username_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.CreateUserAsync("", "ouro", new[] { "foo", "bar" }, "foofoofoo"));
        }

        [Fact]
        public async Task creating_a_user_with_null_name_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.CreateUserAsync("ouro", null, new[] { "foo", "bar" }, "foofoofoo"));
        }

        [Fact]
        public async Task creating_a_user_with_empty_name_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.CreateUserAsync("ouro", "", new[] { "foo", "bar" }, "foofoofoo"));
        }


        [Fact]
        public async Task creating_a_user_with_null_password_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.CreateUserAsync("ouro", "ouro", new[] { "foo", "bar" }, null));
        }

        [Fact]
        public async Task creating_a_user_with_empty_password_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.CreateUserAsync("ouro", "ouro", new[] { "foo", "bar" }, ""));
        }

        [Fact]
        public async Task creating_a_user_with_parameters_can_be_read()
        {
            UserDetails d = null;
            _manager.CreateUserAsync("ouro", "ourofull", new[] {"foo", "bar"}, "ouro", new UserCredentials("admin", "changeit")).Wait();
            d = await _manager.GetUserAsync("ouro", new UserCredentials("admin", "changeit"));
            Assert.Equal("ouro", d.LoginName);
            Assert.Equal("ourofull", d.FullName);
            Assert.Equal("foo", d.Groups[0]);
            Assert.Equal("bar", d.Groups[1]);
        }
    }
}