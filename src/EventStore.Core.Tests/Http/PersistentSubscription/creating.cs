using System.Net;
using EventStore.Core.Tests.Http.Users.users;

using System.Collections.Generic;
using System;
using Xunit;

namespace EventStore.Core.Tests.Http.PersistentSubscription
{
    public class when_creating_a_subscription : with_admin_user
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
        }

        protected override void When()
        {
            var response = MakeJsonPut(
                "/subscriptions/stream/groupname334",
                new
                {
                    ResolveLinkTos = true
                }, _admin);
            Fixture.AddStashedValueAssignment(this, instance =>instance._response = response);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_create()
        {
            Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_location_header()
        {
            Assert.Equal("http://" + Node.ExtHttpEndPoint + "/subscriptions/stream/groupname334",_response.Headers["location"]);
        }
    }

    public class when_creating_a_subscription_without_permissions : with_admin_user
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
        }

        protected override void When()
        {
            var response = MakeJsonPut(
                "/subscriptions/stream/groupname337",
                new
                {
                    ResolveLinkTos = true
                }, null);
            Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_unauthorised()
        {
            Assert.Equal(HttpStatusCode.Unauthorized, _response.StatusCode);
        }
    }

    public class when_creating_a_duplicate_subscription : with_admin_user
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
            var response = MakeJsonPut(
                "/subscriptions/stream/groupname453",
                new
                {
                    ResolveLinkTos = true
                }, _admin);
            Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
        }

        protected override void When()
        {
            var response = MakeJsonPut(
                "/subscriptions/stream/groupname453",
                new
                {
                    ResolveLinkTos = true
                }, _admin);
            Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_conflict()
        {
            Assert.Equal(HttpStatusCode.Conflict, _response.StatusCode);
        }
    }

    
    public class when_creating_a_subscription_with_bad_config : with_admin_user
    {
        protected List<object> Events;
        protected string SubscriptionPath;
        protected string GroupName;
        protected HttpWebResponse Response;

        protected override void Given()
        {
            var events = new List<object>
            {
                new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}},
                new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {B = "2"}},
                new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {C = "3"}},
                new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {D = "4"}}
            };

            var response = MakeArrayEventsPost(
                         TestStream,
                         events,
                         _admin);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance.Events = events;
                instance.Response = response;
            });
        }

        protected override void When()
        {
            var groupName = Guid.NewGuid().ToString();
            var subscriptionPath = string.Format("/subscriptions/{0}/{1}", TestStream.Substring(9), groupName);
            var response = MakeJsonPut(subscriptionPath,
                new
                {
                    ResolveLinkTos = true,
                    BufferSize = 10,
                    ReadBatchSize = 11
                },
                _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance.GroupName =groupName;
                instance.SubscriptionPath = subscriptionPath;
                instance.Response = response;
            });
        }

        [Fact]
        public void returns_bad_request()
        {
            Assert.Equal(HttpStatusCode.BadRequest, Response.StatusCode);
        }
    }
}