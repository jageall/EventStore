using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Transport.Http;
using Xunit;
using Newtonsoft.Json.Linq;
using System.Linq;
using EventStore.Core.Tests.Http.Streams;
using EventStore.Core.Tests.Http.Users.users;
using HttpStatusCode = System.Net.HttpStatusCode;

namespace EventStore.Core.Tests.Http.PersistentSubscription
{
    public abstract class SpecificationWithLongFeed : with_admin_user
    {
        protected int _numberOfEvents = 5;
        protected string SubscriptionGroupName
        {
            get { return "test_subscription_group" + Tag; }
        }
        protected string _subscriptionEndpoint;
        protected List<Guid> _eventIds = new List<Guid>();
        protected void SetupPersistentSubscription(string streamId, string groupName, int messageTimeoutInMs = 10000)
        {
            var subscriptionStream = streamId;
            var subscriptionGroupName =  groupName;
            var subscriptionEndpoint = _subscriptionEndpoint = String.Format("/subscriptions/{0}/{1}", subscriptionStream, subscriptionGroupName);

            var response = MakeJsonPut(
            subscriptionEndpoint,
            new
            {
                ResolveLinkTos = true,
                MessageTimeoutMilliseconds = messageTimeoutInMs
            }, _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._subscriptionEndpoint = subscriptionEndpoint;
            });

            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        protected string PostEvent(int i)
        {
            var eventId = Guid.NewGuid();
            var response = MakeArrayEventsPost(
                TestStream, new[] { new { EventId = eventId, EventType = "event-type", Data = new { Number = i } } });
            _eventIds.Add(eventId);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            return response.Headers[HttpResponseHeader.Location];
        }

        protected override void Given()
        {
            var events = _eventIds = new List<Guid>();
            SetupPersistentSubscription(TestStreamName, SubscriptionGroupName);
            for (var i = 0; i < _numberOfEvents; i++)
            {
                PostEvent(i);
            }
            int numberOfEvents = _numberOfEvents;
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._eventIds = events;
                instance._numberOfEvents = numberOfEvents;
            });
        }

        protected string GetLink(JObject feed, string relation)
        {
            var rel = (from JObject link in feed["links"]
                       from JProperty attr in link
                       where attr.Name == "relation" && (string)attr.Value == relation
                       select link).SingleOrDefault();
            return (rel == null) ? (string)null : (string)rel["uri"];
        }
    }

    
    public class when_retrieving_an_empty_feed : SpecificationWithLongFeed
    {
        private JObject _feed;
        private JObject _head;
        private string _previous;

        protected override void Given()
        {
            base.Given();
            var head = GetJson<JObject>(_subscriptionEndpoint + "/" + _numberOfEvents, ContentType.CompetingJson);
            var previous = _previous= GetLink(head, "previous");
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._head = head;
                instance._previous = previous;
            });
        }

        protected override void When()
        {
            var feed = GetJson<JObject>(_previous, ContentType.CompetingJson);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._feed = feed;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_ok_status_code()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void does_not_contain_ack_all_link()
        {
            var rel = GetLink(_feed, "ackAll");
            Assert.True(string.IsNullOrEmpty(rel));
        }

        [Fact][Trait("Category", "LongRunning")]
        public void does_not_contain_nack_all_link()
        {
            var rel = GetLink(_feed, "nackAll");

            Assert.True(string.IsNullOrEmpty(rel));
        }

        [Fact][Trait("Category", "LongRunning")]
        public void contains_a_link_rel_previous()
        {
            var rel = GetLink(_feed, "previous");
            Assert.NotEmpty(rel);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_feed_is_empty()
        {
            Assert.Equal(0, _feed["entries"].Count());
        }
    }

    
    public class when_retrieving_a_feed_with_events : SpecificationWithLongFeed
    {
        private JObject _feed;
        private List<JToken> _entries;

        protected override void When()
        {
            var allMessagesFeedLink = String.Format("{0}/{1}", _subscriptionEndpoint, _numberOfEvents);
            var feed = GetJson<JObject>(allMessagesFeedLink, ContentType.CompetingJson);
            var entries = feed != null ? feed["entries"].ToList() : new List<JToken>();
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._feed = feed;
                instance._entries = entries;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_ok_status_code()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void contains_all_the_events()
        {
            Assert.Equal(_numberOfEvents, _entries.Count);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_ackAll_link_is_to_correct_uri()
        {
            var ackAllLink = String.Format("subscriptions/{0}/{1}/ack?ids={2}", TestStreamName, SubscriptionGroupName, String.Join(",", _eventIds.ToArray()));
            Assert.Equal(MakeUrl(ackAllLink).ToString(), GetLink(_feed, "ackAll"));
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_nackAll_link_is_to_correct_uri()
        {
            var nackAllLink = String.Format("subscriptions/{0}/{1}/nack?ids={2}", TestStreamName, SubscriptionGroupName, String.Join(",", _eventIds.ToArray()));
            Assert.Equal(MakeUrl(nackAllLink).ToString(), GetLink(_feed, "nackAll"));
        }
    }

    public class when_polling_the_head_forward_and_a_new_event_appears : SpecificationWithLongFeed
    {
        private JObject _feed;
        private string _previous;
        private string _lastEventLocation;
        private List<JToken> _entries;

        protected override void Given()
        {
            base.Given();
            var head = GetJson<JObject>(_subscriptionEndpoint + "/" + _numberOfEvents, ContentType.CompetingJson);
            var previous = _previous = GetLink(head, "previous");
            var lastEventLocation = PostEvent(-1);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._previous = previous;
                instance._lastEventLocation = lastEventLocation;
            });
        }

        protected override void When()
        {
            var feed = GetJson<JObject>(_previous, ContentType.CompetingJson);
            var entries = feed != null ? feed["entries"].ToList() : new List<JToken>();
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._feed = feed;
                instance._entries = entries;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_ok_status_code()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_a_feed_with_a_single_entry_referring_to_the_last_event()
        {
            HelperExtensions.AssertJson(new { entries = new[] { new { Id = _lastEventLocation } } }, _feed);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_ack_link_is_to_correct_uri()
        {
            var link = _entries[0]["links"][2];
            Assert.Equal("ack", link["relation"].ToString());
            var ackLink = String.Format("subscriptions/{0}/{1}/ack/{2}", TestStreamName, SubscriptionGroupName, _eventIds.Last());
            Assert.Equal(MakeUrl(ackLink).ToString(), link["uri"].ToString());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_nack_link_is_to_correct_uri()
        {
            var link = _entries[0]["links"][3];
            Assert.Equal("nack", link["relation"].ToString());
            var ackLink = String.Format("subscriptions/{0}/{1}/nack/{2}", TestStreamName, SubscriptionGroupName, _eventIds.Last());
            Assert.Equal(MakeUrl(ackLink).ToString(), link["uri"].ToString());
        }
    }

    public class when_retrieving_a_feed_with_events_with_competing_xml : SpecificationWithLongFeed
    {
        private XDocument _document;
        private XElement[] _entries;

        protected override void When()
        {
            Get(MakeUrl(_subscriptionEndpoint + "/" + 1).ToString(), String.Empty, ContentType.Competing);
            var document = XDocument.Parse(LastResponseBody);
            var entries = document.GetEntries();

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._document = document;
                instance._entries = entries;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_feed_has_n_events()
        {
            Assert.Equal(1, _entries.Length);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void contains_all_the_events()
        {
            Assert.Equal(1, _entries.Length);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_ackAll_link_is_to_correct_uri()
        {
            var ackAllLink = String.Format("subscriptions/{0}/{1}/ack?ids={2}", TestStreamName, SubscriptionGroupName, String.Join(",", _eventIds[0]));
            Assert.Equal(MakeUrl(ackAllLink).ToString(), _document.Element(XDocumentAtomExtensions.AtomNamespace + "feed").GetLink("ackAll"));
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_nackAll_link_is_to_correct_uri()
        {
            var nackAllLink = String.Format("subscriptions/{0}/{1}/nack?ids={2}", TestStreamName, SubscriptionGroupName, String.Join(",", _eventIds[0]));
            Assert.Equal(MakeUrl(nackAllLink).ToString(), _document.Element(XDocumentAtomExtensions.AtomNamespace + "feed").GetLink("nackAll"));
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_ack_link_is_to_correct_uri()
        {
            var result = _document.Element(XDocumentAtomExtensions.AtomNamespace + "feed")
                                  .Element(XDocumentAtomExtensions.AtomNamespace + "entry")
                                  .GetLink("ack");
            var ackLink = String.Format("subscriptions/{0}/{1}/ack/{2}", TestStreamName, SubscriptionGroupName, _eventIds[0]);
            Assert.Equal(MakeUrl(ackLink).ToString(), result);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_nack_link_is_to_correct_uri()
        {
            var result = _document.Element(XDocumentAtomExtensions.AtomNamespace + "feed")
                                  .Element(XDocumentAtomExtensions.AtomNamespace + "entry")
                                  .GetLink("nack"); ;
            var nackLink = String.Format("subscriptions/{0}/{1}/nack/{2}", TestStreamName, SubscriptionGroupName, _eventIds[0]);
            Assert.Equal(MakeUrl(nackLink).ToString(), result);
        }
    }

    public class when_retrieving_a_feed_with_invalid_content_type : SpecificationWithLongFeed
    {
        protected override void When()
        {
            Get(MakeUrl(_subscriptionEndpoint + "/" + _numberOfEvents).ToString(), String.Empty, ContentType.Xml);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_not_acceptable()
        {
            Assert.Equal(HttpStatusCode.NotAcceptable, LastResponse.StatusCode);
        }
    }
}
