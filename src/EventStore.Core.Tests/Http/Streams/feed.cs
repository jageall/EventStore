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
using HttpStatusCode = System.Net.HttpStatusCode;

namespace EventStore.Core.Tests.Http.Streams
{
    namespace feed
    {
        public abstract class SpecificationWithLongFeed : HttpBehaviorSpecification
        {
            protected override void Given()
            {
                var numberOfEvents = 25;
                for (var i = 0; i < numberOfEvents; i++)
                {
                    PostEvent(i);
                }
            }

            protected string PostEvent(int i)
            {
                var response = MakeArrayEventsPost(
                    TestStream, new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {Number = i}}});
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                return response.Headers[HttpResponseHeader.Location];
            }

            protected string GetLink(JObject feed, string relation)
            {
                var rel = (from JObject link in feed["links"]
                           from JProperty attr in link
                           where attr.Name == "relation" && (string) attr.Value == relation
                           select link).SingleOrDefault();
                return (rel == null) ? (string)null : (string)rel["uri"];
            }
        }

        public class when_posting_multiple_events : SpecificationWithLongFeed
        {
            protected override void When()
            {
                GetJson<JObject>(TestStream, ContentType.AtomJson);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }
        }

        public class when_retrieving_feed_head : SpecificationWithLongFeed
        {
            private JObject _feed;

            protected override void When()
            {
                var feed = GetJson<JObject>(TestStream, ContentType.AtomJson);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void contains_a_link_rel_previous()
            {
                var rel = GetLink(_feed, "previous");
                Assert.NotEmpty(rel);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void contains_a_link_rel_next()
            {
                var rel = GetLink(_feed, "next");
                Assert.NotEmpty(rel);
            }

            [Fact]
            public void contains_a_link_rel_self()
            {
                var rel = GetLink(_feed, "self");
                Assert.NotEmpty(rel);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void contains_a_link_rel_first()
            {
                var rel = GetLink(_feed, "first");
                Assert.NotEmpty(rel);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void contains_a_link_rel_last()
            {
                var rel = GetLink(_feed, "last");
                Assert.NotEmpty(rel);
            }
        }

        public class when_retrieving_the_previous_link_of_the_feed_head: SpecificationWithLongFeed
        {
            private JObject _feed;
            private JObject _head;
            private string _previous;

            protected override void Given()
            {
                base.Given();
                var head = GetJson<JObject>(TestStream, ContentType.AtomJson);
                var previous = _previous = GetLink(head, "previous");
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._head = head;
                    instance._previous = previous;
                });
            }

            protected override void When()
            {
                var feed = GetJson<JObject>(_previous, ContentType.AtomJson);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                });
            }

            [Fact][Trait("Category", "LongRunning")]
            public void returns_200_response()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            [Fact][Trait("Category", "LongRunning")]
            public void there_is_no_prev_link()
            {
                var rel = GetLink(_feed, "prev");
                Assert.Null(rel);
            }

            [Fact][Trait("Category", "LongRunning")]
            public void there_is_a_next_link()
            {
                var rel = GetLink(_feed, "next");
                Assert.NotEmpty(rel);                
            }

            [Fact][Trait("Category", "LongRunning")]
            public void there_is_a_self_link()
            {
                var rel = GetLink(_feed, "self");
                Assert.NotEmpty(rel);
            }

            [Fact][Trait("Category", "LongRunning")]
            public void there_is_a_first_link()
            {
                var rel = GetLink(_feed, "first");
                Assert.NotEmpty(rel);
            }

            [Fact][Trait("Category", "LongRunning")]
            public void there_is_a_last_link()
            {
                var rel = GetLink(_feed, "last");
                Assert.NotEmpty(rel);
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_feed_is_empty()
            {
                Assert.Equal(0, _feed["entries"].Count());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_response_is_not_cachable()
            {
                Assert.Equal("max-age=0, no-cache, must-revalidate", LastResponse.Headers["Cache-Control"]);
            }
        }

        public class when_reading_a_stream_forward_with_deleted_linktos : HttpSpecificationWithLinkToToDeletedEvents
        {
            private JObject _feed;
            private List<JToken> _entries;
            protected override void When()
            {
                var feed = GetJson<JObject>("/streams/" + LinkedStreamName + "/0/forward/10", accept: ContentType.Json);
                var entries = feed != null ? feed["entries"].ToList() : new List<JToken>();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                    instance._entries = entries;
                });
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_feed_has_one_event()
            {
                Assert.Equal(1, _entries.Count());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_edit_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][0];
                Assert.Equal("edit", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_alt_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][1];
                Assert.Equal("alternate", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), foo["uri"].ToString());
            }
        }

        public class when_reading_a_stream_forward_with_linkto : HttpSpecificationWithLinkToToEvents
        {
            private JObject _feed;
            private List<JToken> _entries;
            protected override void When()
            {
                var feed = GetJson<JObject>("/streams/" + LinkedStreamName + "/0/forward/10", accept: ContentType.Json);
                var entries = feed != null ? feed["entries"].ToList() : new List<JToken>();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                    instance._entries = entries;
                });
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_feed_has_two_events()
            {
                Assert.Equal(2, _entries.Count());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_second_edit_link_to_is_to_correct_uri()
            {
                var foo = _entries[1]["links"][0];
                Assert.Equal("edit", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + Stream2Name + "/0"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_second_alt_link_to_is_to_correct_uri()
            {
                var foo = _entries[1]["links"][1];
                Assert.Equal("alternate", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + Stream2Name + "/0"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_first_edit_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][0];
                Assert.Equal("edit", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + StreamName + "/1"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_first_alt_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][1];
                Assert.Equal("alternate", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + StreamName + "/1"), foo["uri"].ToString());
            }
        }

        public class when_reading_a_stream_forward_with_linkto_with_at_sign_in_name : HttpBehaviorSpecification
        {
            protected string LinkedStreamName;
            protected string StreamName;

            protected override void Given()
            {
                var creds = DefaultData.AdminCredentials;
                var linkedStreamName = LinkedStreamName = Guid.NewGuid().ToString();
                var streamName = Guid.NewGuid() + "@" + Guid.NewGuid() + "@";
                using (var conn = TestConnection.Create(Node.TcpEndPoint))
                {
                    conn.ConnectAsync().Wait();
                    conn.AppendToStreamAsync(streamName, ExpectedVersion.Any, creds,
                        new EventData(Guid.NewGuid(), "testing", true, Encoding.UTF8.GetBytes("{'foo' : 4}"), new byte[0]))
                        .Wait();
                    conn.AppendToStreamAsync(streamName, ExpectedVersion.Any, creds,
                        new EventData(Guid.NewGuid(), "testing", true, Encoding.UTF8.GetBytes("{'foo' : 4}"), new byte[0]))
                        .Wait();
                    conn.AppendToStreamAsync(streamName, ExpectedVersion.Any, creds,
                        new EventData(Guid.NewGuid(), "testing", true, Encoding.UTF8.GetBytes("{'foo' : 4}"), new byte[0]))
                        .Wait();
                    conn.AppendToStreamAsync(linkedStreamName, ExpectedVersion.Any, creds,
                        new EventData(Guid.NewGuid(), SystemEventTypes.LinkTo, false,
                            Encoding.UTF8.GetBytes("0@" + streamName), new byte[0])).Wait();
                    conn.AppendToStreamAsync(linkedStreamName, ExpectedVersion.Any, creds,
                        new EventData(Guid.NewGuid(), SystemEventTypes.LinkTo, false,
                            Encoding.UTF8.GetBytes("1@" + streamName), new byte[0])).Wait();

                }

                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance.LinkedStreamName = linkedStreamName;
                    instance.StreamName = streamName;
                });
            }
            private JObject _feed;
            private List<JToken> _entries;
            protected override void When()
            {
                var feed = GetJson<JObject>("/streams/" + LinkedStreamName + "/0/forward/10", accept: ContentType.Json);
                var entries = feed != null ? feed["entries"].ToList() : new List<JToken>();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                    instance._entries = entries;
                });
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_feed_has_two_events()
            {
                Assert.Equal(2, _entries.Count());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_second_edit_link_to_is_to_correct_uri()
            {
                var foo = _entries[1]["links"][0];
                Assert.Equal("edit", foo["relation"].ToString());
                //TODO GFY I really wish we were targeting 4.5 only so I could use Uri.EscapeDataString
                //given the choice between this and a dependency on system.web well yeah. When we have 4.5
                //only lets use Uri
                Assert.Equal(MakeUrlString("/streams/" + StreamName.Replace("@", "%40") + "/0"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_second_alt_link_to_is_to_correct_uri()
            {
                var foo = _entries[1]["links"][1];
                Assert.Equal("alternate", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + StreamName.Replace("@", "%40") + "/0"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_first_edit_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][0];
                Assert.Equal("edit", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + StreamName.Replace("@", "%40") + "/1"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_first_alt_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][1];
                Assert.Equal("alternate", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + StreamName.Replace("@", "%40") + "/1"), foo["uri"].ToString());
            }
        }

        public class when_reading_a_stream_forward_with_maxcount_deleted_linktos : SpecificationWithLinkToToMaxCountDeletedEvents
        {
            private JObject _feed;
            private List<JToken> _entries;
            protected override void When()
            {
                var feed = GetJson<JObject>("/streams/" + LinkedStreamName + "/0/forward/10", accept: ContentType.Json);
                var entries = feed != null ? feed["entries"].ToList() : new List<JToken>();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                    instance._entries = entries;
                });
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_feed_has_no_events()
            {
                Assert.Equal(1, _entries.Count());
            }
        }

        public class when_reading_a_stream_forward_with_maxcount_deleted_linktos_with_rich_entry : SpecificationWithLinkToToMaxCountDeletedEvents
        {
            private JObject _feed;
            private List<JToken> _entries;
            protected override void When()
            {
                var feed = GetJson<JObject>("/streams/" + LinkedStreamName + "/0/forward/10?embed=rich", accept: ContentType.Json); 
                var entries = feed != null ? feed["entries"].ToList() : new List<JToken>();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                    instance._entries = entries;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            [Trait("Category", "Explicit")]
            public void the_feed_has_some_events()
            {
                Assert.Equal(1, _entries.Count());
            }
        }

        public class when_reading_a_stream_forward_with_deleted_linktos_with_content_enabled_as_xml :
            HttpSpecificationWithLinkToToDeletedEvents
        {
            private XDocument _feed;
            private XElement[] _entries;

            protected override void When()
            {

                var feed = GetAtomXml(MakeUrl("/streams/" + LinkedStreamName + "/0/forward/10", "embed=content"));
                var entries = feed.GetEntries();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                    instance._entries = entries;
                });
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_feed_has_one_event()
            {
                Assert.Equal(1, _entries.Length);
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_edit_link_is_to_correct_uri()
            {
                var link = _entries[0].GetLink("edit");
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), link);
            }

        [Fact][Trait("Category", "LongRunning")]
            public void the_alternate_link_is_to_correct_uri()
            {
                var link = _entries[0].GetLink("alternate");
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), link);
            }
        }

        public class when_reading_a_stream_forward_with_deleted_linktos_with_content_enabled : HttpSpecificationWithLinkToToDeletedEvents
        {
            private JObject _feed;
            private List<JToken> _entries;
            protected override void When()
            {
                var uri = MakeUrl("/streams/" + LinkedStreamName + "/0/forward/10", "embed=content");
                var feed = GetJson<JObject>(uri.ToString(), accept: ContentType.Json);
                var entries = feed != null ? feed["entries"].ToList() : new List<JToken>();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                    instance._entries = entries;
                });
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_feed_has_one_event()
            {
                Assert.Equal(1, _entries.Count());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_edit_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][0];
                Assert.Equal("edit", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_alt_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][1];
                Assert.Equal("alternate", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), foo["uri"].ToString());
            }
        }

        public class when_reading_a_stream_backward_with_deleted_linktos : HttpSpecificationWithLinkToToDeletedEvents
        {
            private JObject _feed;
            private List<JToken> _entries;
            protected override void When()
            {
                var feed = GetJson<JObject>("/streams/" + LinkedStreamName + "/0/backward/1", accept: ContentType.Json);
                var entries = feed != null? feed["entries"].ToList() : new List<JToken>();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                    instance._entries = entries;
                });
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_feed_has_one_event()
            {
                Assert.Equal(1, _entries.Count());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_edit_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][0];
                Assert.Equal("edit", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_alt_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][1];
                Assert.Equal("alternate", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), foo["uri"].ToString());
            }
        }

        public class when_reading_a_stream_backward_with_deleted_linktos_and_embed_of_content : HttpSpecificationWithLinkToToDeletedEvents
        {
            private JObject _feed;
            private List<JToken> _entries;
            protected override void When()
            {
                var uri = MakeUrl("/streams/" + LinkedStreamName + "/0/backward/1", "embed=content");
                var feed = GetJson<JObject>(uri.ToString(), accept: ContentType.Json);
                var entries = feed != null ? feed["entries"].ToList() : new List<JToken>();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._feed = feed;
                    instance._entries = entries;
                });
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_feed_has_one_event()
            {
                Assert.Equal(1, _entries.Count());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_edit_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][0];
                Assert.Equal("edit", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), foo["uri"].ToString());
            }

            [Fact][Trait("Category", "LongRunning")]
            public void the_alt_link_to_is_to_correct_uri()
            {
                var foo = _entries[0]["links"][1];
                Assert.Equal("alternate", foo["relation"].ToString());
                Assert.Equal(MakeUrlString("/streams/" + DeletedStreamName + "/0"), foo["uri"].ToString());
            }
        }

        public class when_polling_the_head_forward_and_a_new_event_appears: SpecificationWithLongFeed
        {
            private JObject _feed;
            private JObject _head;
            private string _previous;
            private string _lastEventLocation;

            protected override void Given()
            {
                base.Given();
                var head = GetJson<JObject>(TestStream, ContentType.AtomJson);
        Console.WriteLine(new string('*', 60));
                Console.WriteLine(_head);
                Console.WriteLine(TestStream);
        Console.WriteLine(new string('*', 60));
                var previous = _previous = GetLink(head, "previous");
                var lastEventLocation = PostEvent(-1);

                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._head = head;
                    instance._previous = previous;
                    instance._lastEventLocation = lastEventLocation;
                });
            }

            protected override void When()
            {
                var feed = GetJson<JObject>(_previous, ContentType.AtomJson);
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
            public void returns_a_feed_with_a_single_entry_referring_to_the_last_event()
            {
                HelperExtensions.AssertJson(new {entries = new[] {new {Id = _lastEventLocation}}}, _feed);
            }
        }

    }
}
