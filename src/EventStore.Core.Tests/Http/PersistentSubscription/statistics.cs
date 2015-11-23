using System;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Tests.Http.BasicAuthentication.basic_authentication;
using EventStore.Transport.Http;
using Newtonsoft.Json.Linq;
using Xunit;
using HttpStatusCode = System.Net.HttpStatusCode;
using EventStore.Core.Tests.Helpers;
using System.Xml.Linq;

namespace EventStore.Core.Tests.Http.PersistentSubscription
{
    public class can_get_all_statistics_in_json : with_subscription_having_events
    {
        private JArray _json;

        protected override void When()
        {
            var json = GetJson<JArray>("/subscriptions", accept: ContentType.Json);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._json = json;
            });
        }

        [Fact]
        public void returns_ok()
        {
            Assert.Equal(HttpStatusCode.OK, Fixture.LastResponse.StatusCode);
        }

        [Fact]
        public void body_contains_valid_json()
        {
            Assert.Equal(TestStreamName, _json[0]["eventStreamId"].Value<string>());
        }
    }


    public class when_getting_all_statistics_in_xml : with_subscription_having_events
    {
        private XDocument _xml;

        protected override void When()
        {
            var xml = GetXml(MakeUrl("/subscriptions"));
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._xml = xml;
            });
        }

        [Fact]
        public void returns_ok()
        {
            Assert.Equal(HttpStatusCode.OK, Fixture.LastResponse.StatusCode);
        }

        [Fact]
        public void body_contains_valid_xml()
        {
            Assert.Equal(TestStreamName, _xml.Descendants("EventStreamId").First().Value);
        }
    }

    public class when_getting_non_existent_single_statistics : HttpBehaviorSpecification
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
        }

        protected override void When()
        {
            var request = CreateRequest("/subscriptions/fu/fubar", null, "GET", "text/xml", null);
            var response = GetRequestResponse(request);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_not_found()
        {
            Assert.Equal(HttpStatusCode.NotFound, _response.StatusCode);
        }
    }

    public class when_getting_non_existent_stream_statistics : HttpBehaviorSpecification
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
        }

        protected override void When()
        {
            var request = CreateRequest("/subscriptions/fubar", null, "GET", "text/xml", null);
            var response = GetRequestResponse(request);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_not_found()
        {
            Assert.Equal(HttpStatusCode.NotFound, _response.StatusCode);
        }
    }

    public class when_getting_subscription_statistics_for_individual : SpecificationWithPersistentSubscriptionAndConnections
    {
        private JObject _json;


        protected override void When()
        {
            var json = GetJson<JObject>("/subscriptions/" + _streamName + "/" + _groupName + "/info", ContentType.Json);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._json = json;
            });
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void returns_ok()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void detail_rel_href_is_correct()
        {
            Assert.Equal(string.Format("http://{0}/subscriptions/{1}/{2}/info", Node.ExtHttpEndPoint, _streamName, _groupName),
                _json["links"][0]["href"].Value<string>());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void has_two_rel_links()
        {
            Assert.Equal(2,
                _json["links"].Count());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_view_detail_rel_is_correct()
        {
            Assert.Equal("detail",
                _json["links"][0]["rel"].Value<string>());
        }
        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_event_stream_is_correct()
        {
            Assert.Equal(_streamName, _json["eventStreamId"].Value<string>());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_groupname_is_correct()
        {
            Assert.Equal(_groupName, _json["groupName"].Value<string>());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_status_is_live()
        {
            Assert.Equal("Live", _json["status"].Value<string>());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void there_are_two_connections()
        {
            Assert.Equal(2, _json["connections"].Count());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_first_connection_has_endpoint()
        {
            Assert.NotNull(_json["connections"][0]["from"]);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_second_connection_has_endpoint()
        {
            Assert.NotNull(_json["connections"][1]["from"]);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_first_connection_has_user()
        {
            Assert.Equal("anonymous", _json["connections"][0]["username"].Value<string>());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_second_connection_has_user()
        {
            Assert.Equal("admin", _json["connections"][1]["username"].Value<string>());
        }
    }

    public class when_getting_subscription_stats_summary : SpecificationWithPersistentSubscriptionAndConnections
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                    .DoNotResolveLinkTos()
                                                    .StartFromCurrent();

        private JArray _json;

        protected override void Given()
        {
            base.Given();
            Connection.CreatePersistentSubscriptionAsync(_streamName, "secondgroup", _settings,
                        DefaultData.AdminCredentials).Wait();
            Connection.ConnectToPersistentSubscription(_streamName, "secondgroup",
                        (subscription, @event) => Console.WriteLine(),
                        (subscription, reason, arg3) => Console.WriteLine());
            Connection.ConnectToPersistentSubscription(_streamName, "secondgroup",
                        (subscription, @event) => Console.WriteLine(),
                        (subscription, reason, arg3) => Console.WriteLine(),
                        DefaultData.AdminCredentials);
            Connection.ConnectToPersistentSubscription(_streamName, "secondgroup",
                        (subscription, @event) => Console.WriteLine(),
                        (subscription, reason, arg3) => Console.WriteLine(),
                        DefaultData.AdminCredentials);
            
        }

        protected override void When()
        {
            var json = GetJson<JArray>("/subscriptions", ContentType.Json);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._json = json;
            });
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_response_code_is_ok()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_first_event_stream_is_correct()
        {
            Assert.Equal(_streamName, _json[0]["eventStreamId"].Value<string>());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_first_groupname_is_correct()
        {
            Assert.Equal(_groupName, _json[0]["groupName"].Value<string>());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_first_event_stream_detail_uri_is_correct()
        {
            Assert.Equal(string.Format("http://{0}/subscriptions/{1}/{2}/info", Node.ExtHttpEndPoint, _streamName, _groupName), 
                _json[0]["links"][0]["href"].Value<string>());
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_first_event_stream_detail_has_one_link()
        {
            Assert.Equal(1,
                _json[0]["links"].Count());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_first_event_stream_detail_rel_is_correct()
        {
            Assert.Equal("detail",
                _json[0]["links"][0]["rel"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_event_stream_detail_uri_is_correct()
        {
            Assert.Equal(string.Format("http://{0}/subscriptions/{1}/{2}/info", Node.ExtHttpEndPoint, _streamName, "secondgroup"), 
                _json[1]["links"][0]["href"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_event_stream_detail_has_one_link()
        {
            Assert.Equal(1,
                _json[1]["links"].Count());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_event_stream_detail_rel_is_correct()
        {
            Assert.Equal("detail",
                _json[1]["links"][0]["rel"].Value<string>());
        }
        
        [Fact][Trait("Category", "LongRunning")]
        public void the_first_parked_message_queue_uri_is_correct()
        {
            Assert.Equal(string.Format("http://{0}/streams/$persistentsubscription-{1}::{2}-parked",Node.ExtHttpEndPoint, _streamName, _groupName), _json[0]["parkedMessageUri"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_parked_message_queue_uri_is_correct()
        {
            Assert.Equal(string.Format("http://{0}/streams/$persistentsubscription-{1}::{2}-parked", Node.ExtHttpEndPoint, _streamName, "secondgroup"), _json[1]["parkedMessageUri"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_status_is_live()
        {
            Assert.Equal("Live", _json[0]["status"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void there_are_two_connections()
        {
            Assert.Equal(2, _json[0]["connectionCount"].Value<int>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_subscription_event_stream_is_correct()
        {
            Assert.Equal(_streamName, _json[1]["eventStreamId"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_subscription_groupname_is_correct()
        {
            Assert.Equal("secondgroup", _json[1]["groupName"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void second_subscription_there_are_three_connections()
        {
            Assert.Equal(3, _json[1]["connectionCount"].Value<int>());
        }
    }

    public class when_getting_subscription_stats_for_stream : SpecificationWithPersistentSubscriptionAndConnections
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                    .DoNotResolveLinkTos()
                                                    .StartFromCurrent();

        private JArray _json;
        protected override void Given()
        {
            base.Given();
            Connection.CreatePersistentSubscriptionAsync(_streamName, "secondgroup", _settings,
                        DefaultData.AdminCredentials).Wait();
            Connection.ConnectToPersistentSubscription(_streamName, "secondgroup",
                        (subscription, @event) => Console.WriteLine(),
                        (subscription, reason, arg3) => Console.WriteLine());
            Connection.ConnectToPersistentSubscription(_streamName, "secondgroup",
                        (subscription, @event) => Console.WriteLine(),
                        (subscription, reason, arg3) => Console.WriteLine(),
                        DefaultData.AdminCredentials);
            Connection.ConnectToPersistentSubscription(_streamName, "secondgroup",
                        (subscription, @event) => Console.WriteLine(),
                        (subscription, reason, arg3) => Console.WriteLine(),
                        DefaultData.AdminCredentials);
        }

        protected override void When()
        {
            var json = GetJson<JArray>("/subscriptions/" + _streamName, ContentType.Json);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._json = json;
            });
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_response_code_is_ok()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_first_event_stream_is_correct()
        {
            Assert.Equal(_streamName, _json[0]["eventStreamId"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_first_groupname_is_correct()
        {
            Assert.Equal(_groupName, _json[0]["groupName"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_first_event_stream_detail_uri_is_correct()
        {
            Assert.Equal(string.Format("http://{0}/subscriptions/{1}/{2}/info", Node.ExtHttpEndPoint, _streamName, _groupName), 
                _json[0]["links"][0]["href"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_event_stream_detail_uri_is_correct()
        {
            Assert.Equal(string.Format("http://{0}/subscriptions/{1}/{2}/info", Node.ExtHttpEndPoint, _streamName, "secondgroup"), 
                _json[1]["links"][0]["href"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_first_parked_message_queue_uri_is_correct()
        {
            Assert.Equal(string.Format("http://{0}/streams/$persistentsubscription-{1}::{2}-parked", Node.ExtHttpEndPoint, _streamName, _groupName), _json[0]["parkedMessageUri"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_parked_message_queue_uri_is_correct()
        {
            Assert.Equal(string.Format("http://{0}/streams/$persistentsubscription-{1}::{2}-parked", Node.ExtHttpEndPoint, _streamName, "secondgroup"), _json[1]["parkedMessageUri"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_status_is_live()
        {
            Assert.Equal("Live", _json[0]["status"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void there_are_two_connections()
        {
            Assert.Equal(2, _json[0]["connectionCount"].Value<int>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_subscription_event_stream_is_correct()
        {
            Assert.Equal(_streamName, _json[1]["eventStreamId"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void the_second_subscription_groupname_is_correct()
        {
            Assert.Equal("secondgroup", _json[1]["groupName"].Value<string>());
        }

        [Fact][Trait("Category", "LongRunning")]
        public void second_subscription_there_are_three_connections()
        {
            Assert.Equal(3, _json[1]["connectionCount"].Value<int>());
        }
    }

    public abstract class SpecificationWithPersistentSubscriptionAndConnections : with_admin_user
    {
        protected string _streamName;
        protected string _groupName;
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                    .DoNotResolveLinkTos()
                                                    .StartFromCurrent();

        protected override void Given()
        {
            var streamName = _streamName = Guid.NewGuid().ToString();
            var groupName = _groupName = Guid.NewGuid().ToString();


            Connection.CreatePersistentSubscriptionAsync(_streamName, _groupName, _settings,
                    DefaultData.AdminCredentials).Wait();
            Connection.ConnectToPersistentSubscription(_streamName, _groupName,
                        (subscription, @event) => Console.WriteLine(), 
                        (subscription, reason, arg3) => Console.WriteLine());
            Connection.ConnectToPersistentSubscription(_streamName, _groupName,
                        (subscription, @event) => Console.WriteLine(),
                        (subscription, reason, arg3) => Console.WriteLine(),
                        DefaultData.AdminCredentials);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._groupName = groupName;
                instance._streamName= streamName;
            });
        }

        protected override void When()
        {

        }
    }
}