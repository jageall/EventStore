using System;
using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using EventStore.ClientAPI.Transport.Http;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement
{
    public class deleting_a_user : TestWithNode
    {
        [Fact]
        public void deleting_non_existing_user_throws()
        {
            var ex = Assert.Throws<AggregateException>(() => _manager.DeleteUserAsync(Guid.NewGuid().ToString(), new UserCredentials("admin", "changeit")).Wait());
            var realex = (UserCommandFailedException) ex.InnerException;
            Assert.Equal(HttpStatusCode.NotFound, realex.HttpStatusCode);
        }

        [Fact]
        public void deleting_created_user_deletes_it()
        {
            var user = Guid.NewGuid().ToString();
            _manager.CreateUserAsync(user, "ourofull", new[] { "foo", "bar" }, "ouro", new UserCredentials("admin", "changeit")).Wait();
            _manager.DeleteUserAsync(user, new UserCredentials("admin", "changeit")).Wait();
        }


        [Fact]
        public async Task deleting_null_user_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.DeleteUserAsync(null, new UserCredentials("admin", "changeit")));
        }

        [Fact]
        public async Task deleting_empty_user_throws()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => _manager.DeleteUserAsync("", new UserCredentials("admin", "changeit")));
        }

        [Fact]
        public async Task can_delete_a_user()
        {
            _manager.CreateUserAsync("ouro", "ouro", new[] { "foo", "bar" }, "ouro", new UserCredentials("admin", "changeit")).Wait();


            await _manager.GetUserAsync("ouro", new UserCredentials("admin", "changeit"));
            
            _manager.DeleteUserAsync("ouro", new UserCredentials("admin", "changeit")).Wait();
            
            var ex = await Assert.ThrowsAsync<AggregateException>(() => _manager.GetUserAsync("ouro", new UserCredentials("admin", "changeit")));
            var inner = Assert.IsType<UserCommandFailedException>(ex.InnerException);
            Assert.Equal(HttpStatusCode.NotFound, inner.HttpStatusCode);
        }
    }
}