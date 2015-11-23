using System.Net;
using System.Security.Principal;
using EventStore.Core.Messages;
using EventStore.Core.Services.Transport.Http.Authentication;
using EventStore.Core.Services.Transport.Http.Messages;
using EventStore.Core.Tests.Authentication;
using EventStore.Transport.Http.EntityManagement;
using Xunit;
using System.Linq;

namespace EventStore.Core.Tests.Services.Transport.Http.Authentication
{
    namespace basic_http_authentication_provider
    {
        public class TestFixtureWithBasicHttpAuthenticationProvider: with_internal_authentication_provider
        {
            protected BasicHttpAuthenticationProvider _provider;
            protected HttpEntity _entity;

            protected new void SetUpProvider()
            {
                base.SetUpProvider();
                _provider = new BasicHttpAuthenticationProvider(_internalAuthenticationProvider);
            }
        }

        public class when_handling_a_request_without_an_authorization_header : TestFixtureWithBasicHttpAuthenticationProvider
        {
            private bool _authenticateResult;

            public when_handling_a_request_without_an_authorization_header()
            {
                SetUpProvider();
                _entity = HttpEntity.Test(null);
                _authenticateResult = _provider.Authenticate(new IncomingHttpRequestMessage(null, _entity, _bus));
            }

            [Fact]
            public void returns_false()
            {
                Assert.False(_authenticateResult);
            }

            [Fact]
            public void does_not_publish_authenticated_http_request_message()
            {
                var authenticatedHttpRequestMessages = _consumer.HandledMessages.OfType<AuthenticatedHttpRequestMessage>().ToList();
                Assert.Equal(0, authenticatedHttpRequestMessages.Count);
            }
        }

        public class when_handling_a_request_with_correct_user_name_and_password : TestFixtureWithBasicHttpAuthenticationProvider
        {
            private bool _authenticateResult;

            protected override void Given()
            {
                base.Given();
                ExistingEvent("$user-user", "$user", null, "{LoginName:'user', Salt:'drowssap',Hash:'password'}");
            }

            public when_handling_a_request_with_correct_user_name_and_password()
            {
                SetUpProvider();
                _entity = HttpEntity.Test(new GenericPrincipal(new HttpListenerBasicIdentity("user", "password"), new string[0]));
                _authenticateResult = _provider.Authenticate(new IncomingHttpRequestMessage(null, _entity, _bus));
            }

            [Fact]
            public void returns_true()
            {
                Assert.True(_authenticateResult);
            }

            [Fact]
            public void publishes_authenticated_http_request_message_with_user()
            {
                var authenticatedHttpRequestMessages = _consumer.HandledMessages.OfType<AuthenticatedHttpRequestMessage>().ToList();
                Assert.Equal(1, authenticatedHttpRequestMessages.Count);
                var message = authenticatedHttpRequestMessages[0];
                Assert.Equal("user", message.Entity.User.Identity.Name);
                Assert.True(message.Entity.User.Identity.IsAuthenticated);
            }
        }

        public class when_handling_multiple_requests_with_the_same_correct_user_name_and_password : TestFixtureWithBasicHttpAuthenticationProvider
        {
            private bool _authenticateResult;

            protected override void Given()
            {
                base.Given();
                ExistingEvent("$user-user", "$user", null, "{LoginName:'user', Salt:'drowssap',Hash:'password'}");
            }

            public when_handling_multiple_requests_with_the_same_correct_user_name_and_password()
            {
                SetUpProvider();

                var entity = HttpEntity.Test(new GenericPrincipal(new HttpListenerBasicIdentity("user", "password"), new string[0]));
                _provider.Authenticate(new IncomingHttpRequestMessage(null, entity, _bus));

                _consumer.HandledMessages.Clear();

                _entity = HttpEntity.Test(new GenericPrincipal(new HttpListenerBasicIdentity("user", "password"), new string[0]));
                _authenticateResult = _provider.Authenticate(new IncomingHttpRequestMessage(null, _entity, _bus));
            }

            [Fact]
            public void returns_true()
            {
                Assert.True(_authenticateResult);
            }

            [Fact]
            public void publishes_authenticated_http_request_message_with_user()
            {
                var authenticatedHttpRequestMessages = _consumer.HandledMessages.OfType<AuthenticatedHttpRequestMessage>().ToList();
                Assert.Equal(1, authenticatedHttpRequestMessages.Count);
                var message = authenticatedHttpRequestMessages[0];
                Assert.Equal("user", message.Entity.User.Identity.Name);
                Assert.True(message.Entity.User.Identity.IsAuthenticated);
            }

            [Fact]
            public void does_not_publish_any_read_requests()
            {
                Assert.Equal(0, _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsBackward>().Count());
                Assert.Equal(0, _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Count());
            }
        }

        public class when_handling_a_request_with_incorrect_user_name_and_password : TestFixtureWithBasicHttpAuthenticationProvider
        {
            private bool _authenticateResult;

            protected override void Given()
            {
                base.Given();
                ExistingEvent("$user-user", "$user", null, "{LoginName:'user', Salt:'drowssap',Hash:'password'}");
            }

            public when_handling_a_request_with_incorrect_user_name_and_password()
            {
                SetUpProvider();
                _entity = HttpEntity.Test(new GenericPrincipal(new HttpListenerBasicIdentity("user", "password1"), new string[0]));
                _authenticateResult = _provider.Authenticate(new IncomingHttpRequestMessage(null, _entity, _bus));
            }

            [Fact]
            public void returns_true()
            {
                Assert.True(_authenticateResult);
            }

            [Fact]
            public void publishes_authenticated_http_request_message_with_user()
            {
                var authenticatedHttpRequestMessages = _consumer.HandledMessages.OfType<AuthenticatedHttpRequestMessage>().ToList();
                Assert.Equal(0, authenticatedHttpRequestMessages.Count);
            }
        }
    }

    
}

