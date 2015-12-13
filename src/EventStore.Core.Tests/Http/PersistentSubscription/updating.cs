using System;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Tests.Http.Users.users;
using Xunit;

namespace EventStore.Core.Tests.Http.PersistentSubscription
{
    public class when_updating_a_subscription_without_permissions : with_admin_user
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
            _response = MakeJsonPut(
                "/subscriptions/stream/groupname337",
                new
                {
                    ResolveLinkTos = true
                }, _admin);
        }

        protected override void When()
        {
            _response = MakeJsonPost(
                "/subscriptions/stream/groupname337",
                new
                {
                    ResolveLinkTos = true
                }, null);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_unauthorised()

        {
            Assert.Equal(HttpStatusCode.Unauthorized, _response.StatusCode);
        }

        public when_updating_a_subscription_without_permissions(SpecificationFixture data) : base(data)
        {
        }
    }

    public class when_updating_a_non_existent_subscription_without_permissions : with_admin_user
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
        }

        protected override void When()
        {
            _response = MakeJsonPost(
                "/subscriptions/stream/groupname3337",
                new
                {
                    ResolveLinkTos = true
                }, new NetworkCredential("admin", "changeit"));
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_not_found()
        {
            Assert.Equal(HttpStatusCode.NotFound, _response.StatusCode);
        }

        public when_updating_a_non_existent_subscription_without_permissions(SpecificationFixture data) : base(data)
        {
        }
    }

    public class when_updating_a_existent_subscription : with_admin_user
    {
        private HttpWebResponse _response;
        private string _groupName;
        private SubscriptionDropReason _droppedReason;
        private Exception _exception;
        private const string _stream = "stream";
        private AutoResetEvent _dropped = new AutoResetEvent(false);

        protected override void Given()
        {
            var groupName = _groupName = Guid.NewGuid().ToString();
            var response = MakeJsonPut(
                string.Format("/subscriptions/{0}/{1}", _stream, groupName),
                new
                {
                    ResolveLinkTos = true
                }, DefaultData.AdminNetworkCredentials);
            SetupSubscription();
            //TODO: JAG this seems pretty pointless as it is immediately overwritten by when
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
                instance._groupName = groupName;
            });
        }

        private void SetupSubscription()
        {
            var dropped = new AutoResetEvent(false);
            SubscriptionDropReason droppedReason = SubscriptionDropReason.Unknown;
            Exception exception = null;
            Connection.ConnectToPersistentSubscription(_stream,_groupName, (x, y) => { },
                (sub, reason, ex) =>
                {
                    droppedReason = _droppedReason = reason;
                    exception = _exception = ex;
                    dropped.Set();
                }, DefaultData.AdminCredentials);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._dropped = dropped;
                instance._droppedReason = droppedReason;
                instance._exception = exception;
            });
        }

        protected override void When()
        {
            var response = MakeJsonPost(
                string.Format("/subscriptions/{0}/{1}", _stream, _groupName),
                new
                {
                    ResolveLinkTos = true
                }, DefaultData.AdminNetworkCredentials);

            Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_ok()
        {
            Assert.Equal(HttpStatusCode.OK, _response.StatusCode);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void existing_subscriptions_are_dropped()
        {
            Assert.True(_dropped.WaitOne(TimeSpan.FromSeconds(5)));
            Assert.Equal(SubscriptionDropReason.UserInitiated, _droppedReason);
            Assert.Null(_exception);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void location_header_is_present()
        {
            Assert.Equal(string.Format("http://{0}/subscriptions/{1}/{2}", Node.ExtHttpEndPoint, _stream, _groupName), _response.Headers["Location"]);
        }

        public when_updating_a_existent_subscription(SpecificationFixture data) : base(data)
        {
        }
    }
}