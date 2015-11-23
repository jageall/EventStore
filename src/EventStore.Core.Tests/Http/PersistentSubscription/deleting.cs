using System;
using System.Net;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.Core.Tests.Http.Users.users;
using Xunit;

namespace EventStore.Core.Tests.Http.PersistentSubscription
{
    public class when_deleting_non_existing_subscription : with_admin_user
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
            var response = MakeJsonPut(
                "/subscriptions/stream/groupname156",
                new
                {
                    ResolveLinkTos = true
                }, _admin);
            Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
        }

        protected override void When()
        {
            var req = CreateRequest("/subscriptions/stream/groupname158", "DELETE", _admin);
            var response = GetRequestResponse(req);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact]
	[Trait("Category", "LongRunning")]
        public void returns_notfound()
        {
            Assert.Equal(HttpStatusCode.NotFound, _response.StatusCode);
        }
    }

    public class when_deleting_an_existing_subscription : with_admin_user
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
            var response = MakeJsonPut(
                "/subscriptions/stream/groupname156",
                new
                {
                    ResolveLinkTos = true
                }, _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        protected override void When()
        {
            var req = CreateRequest("/subscriptions/stream/groupname156", "DELETE", _admin);
            var response = GetRequestResponse(req);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_ok_status_code()
        {
            Assert.Equal(HttpStatusCode.OK, _response.StatusCode);
        }
    }

    public class when_deleting_an_existing_subscription_without_permissions : with_admin_user
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
            var response = MakeJsonPut(
                "/subscriptions/stream/groupname156",
                new
                {
                    ResolveLinkTos = true
                }, _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        protected override void When()
        {
            var req = CreateRequest("/subscriptions/stream/groupname156", "DELETE");
            var response = GetRequestResponse(req);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
            Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
        }

        [Fact]
        public void returns_unauthorized()
        {
            Assert.Equal(HttpStatusCode.Unauthorized, _response.StatusCode);
        }
    }

    public class when_deleting_an_existing_subscription_with_subscribers : with_admin_user
    {
        private HttpWebResponse _response;
        private const string _stream = "astreamname";
        private string _groupName;
        private SubscriptionDropReason _reason;
        private Exception _exception;
        private AutoResetEvent _dropped;

        protected override void Given()
        {
            var dropped = new AutoResetEvent(false);
            SubscriptionDropReason reason = SubscriptionDropReason.Unknown;
            var groupName = _groupName = Guid.NewGuid().ToString();
            Exception exception = null;
            var response = MakeJsonPut(
                string.Format("/subscriptions/{0}/{1}", _stream, _groupName),
                new
                {
                    ResolveLinkTos = true
                }, _admin);
            Connection.ConnectToPersistentSubscription(_stream, _groupName, (x, y) => { },
                // ReSharper disable once ImplicitlyCapturedClosure pretty important it captures it really
                (sub, r, e) =>
                {
                    dropped.Set();
                    reason = r;
                    exception = e;
                });
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
                instance._dropped = dropped;
                instance._exception = exception;
                instance._reason = reason;
                instance._groupName = groupName;
            });
        }

        protected override void When()
        {
            var req = CreateRequest(string.Format("/subscriptions/{0}/{1}", _stream, _groupName), "DELETE", _admin);
            var response = GetRequestResponse(req);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact]
        public void returns_ok()
        {
            Assert.Equal(HttpStatusCode.OK, _response.StatusCode);
        }

        [Fact]
        public void the_subscription_is_dropped()
        {
            Assert.True(_dropped.WaitOne(TimeSpan.FromSeconds(5)));
            Assert.Equal(SubscriptionDropReason.UserInitiated, _reason);
            Assert.Null(_exception);
        }
    }
}
