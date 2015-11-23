using System;
using System.Net;
using System.Text.RegularExpressions;
using EventStore.Core.Tests.Http.Users.users;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;
using EventStore.Transport.Http;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common;
using Xunit;

// ReSharper disable InconsistentNaming

namespace EventStore.Core.Tests.Http.PersistentSubscription
{
    public class with_subscription_having_events : with_admin_user
    {
        protected List<object> Events;
        protected string SubscriptionPath;
        protected string GroupName;
        protected int? NumberOfEventsToCreate;

        protected override void Given()
        {
            var events = Events = new List<object>
            {
                new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}},
                new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {B = "2"}},
                new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {C = "3"}},
                new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {D = "4"}}
            };

            var response = MakeArrayEventsPost(
                         TestStream,
                         events.Take(NumberOfEventsToCreate ?? events.Count),
                         _admin);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);

            var groupName = GroupName = Guid.NewGuid().ToString();
            var subscriptionPath = SubscriptionPath = string.Format("/subscriptions/{0}/{1}", TestStream.Substring(9), groupName);
            response = MakeJsonPut(subscriptionPath,
                new
                {
                    ResolveLinkTos = true,
                    MessageTimeoutMilliseconds = 10000,
                },
                _admin);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance.Events = events;
                instance.GroupName = groupName;
                instance.SubscriptionPath = subscriptionPath;
            });
        }

        protected override void When()
        {

        }

        protected void SecureStream()
        {
            var metadata =
                (StreamMetadata)
                StreamMetadata.Build()
                              .SetMetadataReadRole("admin")
                              .SetMetadataWriteRole("admin")
                              .SetReadRole("admin")
                              .SetWriteRole("admin");
            var jsonMetadata = metadata.AsJsonString();
            var response = MakeArrayEventsPost(
                TestMetadataStream,
                new[]
                    {
                            new
                                {
                                    EventId = Guid.NewGuid(),
                                    EventType = SystemEventTypes.StreamMetadata,
                                    Data = new JRaw(jsonMetadata)
                                }
                    });
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
    }

    public class when_getting_messages_without_permission : with_subscription_having_events
    {
        protected override void Given()
        {
            base.Given();
            SecureStream();
        }
        protected override void When()
        {
            GetJson<JObject>(
               SubscriptionPath,
               ContentType.CompetingJson);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_unauthorised()
        {
            Assert.Equal(HttpStatusCode.Unauthorized, LastResponse.StatusCode);
        }
    }

    public class when_getting_messages_from_an_empty_subscription : with_admin_user
    {
        private JObject _response;
        protected string SubscriptionPath;
        
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

            var groupName = Guid.NewGuid().ToString();
            var subscriptionPath = SubscriptionPath = string.Format("/subscriptions/{0}/{1}", TestStream.Substring(9), groupName);
            response = MakeJsonPut(subscriptionPath,
                new
                {
                    ResolveLinkTos = true,
                    MessageTimeoutMilliseconds = 10000
                },
                _admin);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);

            //pull all events out.
            var json = GetJson<JObject>(
                       subscriptionPath + "/" + events.Count,
                       ContentType.CompetingJson, //todo CLC sort out allowed content types
                       _admin);

            var count = ((JObject)json)["entries"].Count();
            Assert.Equal(events.Count, count);
        }

        protected override void When()
        {
            var response = GetJson<JObject>(
                      SubscriptionPath,
                      ContentType.CompetingJson,
                      _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void return_0_messages()
        {
            var count = ((JObject)_response)["entries"].Count();
            Assert.Equal(0, count);
        }
    }

    public class when_getting_messages_from_a_subscription_with_n_messages : with_subscription_having_events
    {
        private JObject _response;

        protected override void When()
        {

            var response = GetJson<JObject>(
                                SubscriptionPath + "/" + Events.Count,
                                ContentType.CompetingJson, //todo CLC sort out allowed content types
                                _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_n_messages()
        {
            var count = ((JObject)_response)["entries"].Count();
            Assert.Equal(Events.Count, count);
        }
    }

    public class when_getting_messages_from_a_subscription_with_more_than_n_messages : with_subscription_having_events
    {
        private JObject _response;

        protected override void When()
        {

            var response = GetJson<JObject>(
                                SubscriptionPath + "/" + (Events.Count - 1),
                                ContentType.CompetingJson, //todo CLC sort out allowed content types
                                _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_n_messages()
        {
            var count = ((JArray)_response["entries"]).Count;
            Assert.Equal(Events.Count - 1, count);
        }
    }

    public class when_getting_messages_from_a_subscription_with_less_than_n_messags : with_subscription_having_events
    {
        private JObject _response;

        protected override void When()
        {
            var response = GetJson<JObject>(
                                SubscriptionPath + "/" + (Events.Count + 1),
                                ContentType.CompetingJson, //todo CLC sort out allowed content types
                                _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_all_messages()
        {
            var count = ((JArray)_response["entries"]).Count;
            Assert.Equal(Events.Count, count);
        }
    }
   
    public class when_getting_messages_from_a_subscription_with_unspecified_count : with_subscription_having_events
    {
        private JObject _response;

        protected override void When()
        {

            var response = GetJson<JObject>(
                                SubscriptionPath,
                                ContentType.CompetingJson,
                                _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_1_message()
        {
            var count = ((JArray)_response["entries"]).Count;
            Assert.Equal(1, count);
        }
    }

    public class when_getting_messages_from_a_subscription_with_a_negative_count : with_subscription_having_events
    {
        protected override void When()
        {
            Get(SubscriptionPath + "/-1",
                "",
                ContentType.CompetingJson,
                _admin);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_bad_request()
        {
            Assert.Equal(HttpStatusCode.BadRequest, LastResponse.StatusCode);
        }
    }

    public class when_getting_messages_from_a_subscription_with_a_count_of_0 : with_subscription_having_events
    {
        protected override void When()
        {
            Get(SubscriptionPath + "/0",
              "",
              ContentType.CompetingJson,
              _admin);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_bad_request()
        {
            Assert.Equal(HttpStatusCode.BadRequest, LastResponse.StatusCode);
        }
    }

    public class when_getting_messages_from_a_subscription_with_count_more_than_100 : with_subscription_having_events
    {
        protected override void When()
        {
            Get(SubscriptionPath + "/101",
                "",
                ContentType.CompetingJson,
                _admin);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_bad_request()
        {
            Assert.Equal(HttpStatusCode.BadRequest, LastResponse.StatusCode);
        }
    }

    public class when_getting_messages_from_a_subscription_with_count_not_an_integer : with_subscription_having_events
    {
        protected override void When()
        {
            Get(SubscriptionPath + "/10.1",
              "",
              ContentType.CompetingJson,
              _admin);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_bad_request()
        {
            Assert.Equal(HttpStatusCode.BadRequest, LastResponse.StatusCode);
        }
    }

    public class when_getting_messages_from_a_subscription_with_count_not_a_number : with_subscription_having_events
    {
        protected override void When()
        {
            Get(SubscriptionPath + "/one",
            "",
            ContentType.CompetingJson,
            _admin);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_bad_request()
        {
            Assert.Equal(HttpStatusCode.BadRequest, LastResponse.StatusCode);
        }
    }
}