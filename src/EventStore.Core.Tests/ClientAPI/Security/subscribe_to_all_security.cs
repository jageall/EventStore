using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security
{
    public class subscribe_to_all_security : AuthenticationTestBase
    {
        public subscribe_to_all_security(Fixture fixture) : base(fixture)
        {
            
        }
        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void subscribing_to_all_with_not_existing_credentials_is_not_authenticated()
        {
            Expect<NotAuthenticatedException>(() => SubscribeToAll("badlogin", "badpass"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void subscribing_to_all_with_no_credentials_is_denied()
        {
            Expect<AccessDeniedException>(() => SubscribeToAll(null, null));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void subscribing_to_all_with_not_authorized_user_credentials_is_denied()
        {
            Expect<AccessDeniedException>(() => SubscribeToAll("user2", "pa$$2"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void subscribing_to_all_with_authorized_user_credentials_succeeds()
        {
            ExpectNoException(() => SubscribeToAll("user1", "pa$$1"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void subscribing_to_all_with_admin_user_credentials_succeeds()
        {
            ExpectNoException(() => SubscribeToAll("adm", "admpa$$"));
        }
    }
}