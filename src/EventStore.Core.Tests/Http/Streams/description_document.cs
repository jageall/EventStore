using System;
using System.Net;
using System.Text;
using EventStore.Core.Tests.ClientAPI;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Tests.Http.Users;
using EventStore.Transport.Http;
using System.Linq;
using Newtonsoft.Json.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;
using EventStore.Core.Services.Transport.Http;
using System.Collections.Generic;
using System.ComponentModel;
using Xunit;

namespace EventStore.Core.Tests.Http.Streams
{
    public class when_getting_a_stream_without_accept_header : HttpBehaviorSpecification
    {
        private JObject _descriptionDocument;
        private List<JToken> _links;
        protected override void Given() { }

        protected override void When()
        {
            var descriptionDocument = GetJsonWithoutAcceptHeader<JObject>(TestStream);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._descriptionDocument = descriptionDocument;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_not_acceptable()
        {
            Assert.Equal(HttpStatusCode.NotAcceptable, LastResponse.StatusCode);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_a_description_document()
        {
            Assert.NotNull(_descriptionDocument);
            _links = _descriptionDocument != null ? _descriptionDocument["_links"].ToList() : new List<JToken>();
            Assert.NotNull(_links);
        }
    }

    public class when_getting_a_stream_with_description_document_media_type : HttpBehaviorSpecification
    {
        private JObject _descriptionDocument;
        private List<JToken> _links;
        protected override void Given() { }

        protected override void When()
        {
            var descriptionDocument = GetJson<JObject>(TestStream, "application/vnd.eventstore.streamdesc+json", null);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._descriptionDocument = descriptionDocument;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_ok()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_a_description_document()
        {
            Assert.NotNull(_descriptionDocument);
            _links = _descriptionDocument != null ? _descriptionDocument["_links"].ToList() : new List<JToken>();
            Assert.NotNull(_links);
        }
    }

    public class when_getting_description_document : HttpBehaviorSpecification
    {
        private JObject _descriptionDocument;
        private List<JToken> _links;
        protected override void Given() { }

        protected override void When()
        {
            var descriptionDocument = GetJson<JObject>(TestStream, "application/vnd.eventstore.streamdesc+json", null);
            var links = descriptionDocument != null ? descriptionDocument["_links"].ToList() : new List<JToken>();
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._descriptionDocument = descriptionDocument;
                instance._links = links;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_ok()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_a_description_document()
        {
            Assert.NotNull(_descriptionDocument);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void contains_the_self_link()
        {
            Assert.Equal("self", ((JProperty)_links[0]).Name);
            Assert.Equal(TestStream, _descriptionDocument["_links"]["self"]["href"].ToString());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void self_link_contains_only_the_description_document_content_type()
        {
            var supportedContentTypes = _descriptionDocument["_links"]["self"]["supportedContentTypes"].Values<string>().ToArray();
            Assert.Equal(1, supportedContentTypes.Length);
            Assert.Equal("application/vnd.eventstore.streamdesc+json", supportedContentTypes[0]);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void contains_the_stream_link()
        {
            Assert.Equal("stream", ((JProperty)_links[1]).Name);
            Assert.Equal(TestStream, _descriptionDocument["_links"]["stream"]["href"].ToString());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void stream_link_contains_supported_stream_content_types()
        {
            var supportedContentTypes = _descriptionDocument["_links"]["stream"]["supportedContentTypes"].Values<string>().ToArray();
            Assert.Equal(2, supportedContentTypes.Length);
            Assert.Contains("application/atom+xml", supportedContentTypes);
            Assert.Contains("application/vnd.eventstore.atom+json", supportedContentTypes);
        }
    }

    public class when_getting_description_document_and_subscription_exists_for_stream : HttpBehaviorSpecification
    {
        private JObject _descriptionDocument;
        private List<JToken> _links;
        private JToken[] _subscriptions;
        private string _subscriptionUrl;
        protected override void Given()
        {
            var subscriptionUrl = "/subscriptions/" + TestStreamName + "/groupname334";
            MakeJsonPut(
                subscriptionUrl,
                new
                {
                    ResolveLinkTos = true
                }, DefaultData.AdminNetworkCredentials);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._subscriptionUrl = subscriptionUrl;
            });
        }

        protected override void When()
        {
            var descriptionDocument = GetJson<JObject>(TestStream, "application/vnd.eventstore.streamdesc+json", null);
            var links = descriptionDocument != null ? descriptionDocument["_links"].ToList() : new List<JToken>();
            var subscriptions = descriptionDocument["_links"]["streamSubscription"].Values<JToken>().ToArray();
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._descriptionDocument = descriptionDocument;
                instance._links = links;
                instance._subscriptions = subscriptions;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_ok()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void returns_a_description_document()
        {
            Assert.NotNull(_descriptionDocument);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void contains_3_links()
        {
            Assert.Equal(3, _links.Count);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void contains_the_subscription_link()
        {
            Assert.Equal("streamSubscription", ((JProperty)_links[2]).Name);
            Assert.Equal(_subscriptionUrl, _subscriptions[0]["href"].ToString());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void subscriptions_link_contains_supported_subscription_content_types()
        {
            var supportedContentTypes = _subscriptions[0]["supportedContentTypes"].Values<string>().ToArray();
            Assert.Equal(2, supportedContentTypes.Length);
            Assert.Contains("application/vnd.eventstore.competingatom+xml", supportedContentTypes);
            Assert.Contains("application/vnd.eventstore.competingatom+json", supportedContentTypes);
        }
    }
}
