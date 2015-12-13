using System;
using System.ComponentModel;
using System.Net;
using System.Text;
using EventStore.Core.Tests.ClientAPI;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Tests.Http.Users;
using EventStore.Transport.Http;
using Xunit;
using Newtonsoft.Json.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;

namespace EventStore.Core.Tests.Http.Streams
{
    namespace basic
    {
        public class when_posting_an_event_as_raw_json_without_eventtype : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                _response = MakeJsonPost(
                    TestStream,
                    new {A="1", B="3", C="5"});
            }

            [Fact]
            public void returns_created_status_code()
            {
                Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
            }

            public when_posting_an_event_as_raw_json_without_eventtype(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_to_idempotent_uri_as_events_array : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                _response = MakeArrayEventsPost(
                    TestStream + "/incoming/" + Guid.NewGuid().ToString(),
                    new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
            }

            [Fact]
            public void returns_bad_request_status_code()
            {
                Assert.Equal(HttpStatusCode.UnsupportedMediaType, _response.StatusCode);
            }

            public when_posting_an_event_to_idempotent_uri_as_events_array(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_as_json_to_idempotent_uri_without_event_type : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var response = MakeJsonPost(
                    TestStream + "/incoming/" + Guid.NewGuid().ToString(),
                    new {A="1", B="3", C="5"});
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            public void returns_bad_request_status_code()
            {
                Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
            }

            public when_posting_an_event_as_json_to_idempotent_uri_without_event_type(SpecificationFixture data) : base(data)
            {
            }
        }


        public class when_posting_an_event_in_json_to_idempotent_uri_without_event_id : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var request = CreateRequest(TestStream + "/incoming/" + Guid.NewGuid().ToString(), "", "POST", "application/json");
                request.Headers.Add("ES-EventType", "SomeType");
                request.AllowAutoRedirect = false;
                var data = "{a : \"1\", b:\"3\", c:\"5\" }";
                var bytes = Encoding.UTF8.GetBytes(data);
                request.ContentLength = data.Length;
                request.GetRequestStream().Write(bytes, 0, data.Length);
                var response = GetRequestResponse(request);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            public void returns_created_status_code()
            {
                Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
            }

            [Fact]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);;
            }

            [Fact]
            public void returns_a_location_header_that_can_be_read_as_json()
            {
                var json = GetJson<JObject>(_response.Headers[HttpResponseHeader.Location]);
                HelperExtensions.AssertJson(new {a = "1", b="3", c="5"}, json);
            }

            public when_posting_an_event_in_json_to_idempotent_uri_without_event_id(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_as_raw_json_without_eventid : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var request = CreateRequest(TestStream, "", "POST", "application/json");
                request.Headers.Add("ES-EventType", "SomeType");
                request.AllowAutoRedirect = false;
                var data = "{A : \"1\", B:\"3\", C:\"5\" }";
                var bytes = Encoding.UTF8.GetBytes(data);
                request.ContentLength = data.Length;
                request.GetRequestStream().Write(bytes, 0, data.Length);
                var response = GetRequestResponse(request);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            public void returns_redirectkeepverb_status_code()
            {
                Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
            }

            [Fact]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            public void returns_a_to_incoming()
            {
                Assert.True(_response.Headers[HttpResponseHeader.Location].Contains("/incoming/"));
                //HelperExtensions.AssertJson(new {A = "1"}, json);
            }

            public when_posting_an_event_as_raw_json_without_eventid(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_as_array_with_no_event_type : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var response = MakeArrayEventsPost(
                    TestStream,
                    new[] {new {EventId = Guid.NewGuid(), Data = new {A = "1"}}});
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            public void returns_bad_request_status_code()
            {
                Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
            }

            public when_posting_an_event_as_array_with_no_event_type(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_as_array : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var response = MakeArrayEventsPost(
                    TestStream,
                    new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
                
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            public void returns_created_status_code()
            {
                Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
            }

            [Fact]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            public void returns_a_location_header_that_can_be_read_as_json()
            {
                var json = GetJson<JObject>(_response.Headers[HttpResponseHeader.Location]);
                HelperExtensions.AssertJson(new {A = "1"}, json);
            }

            public when_posting_an_event_as_array(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_as_array_to_stream_with_slash : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var request = CreateRequest(TestStream + "/", "", "POST", "application/vnd.eventstore.events+json", null);
                request.AllowAutoRedirect = false;
                request.GetRequestStream()
                    .WriteJson(new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
                var response = (HttpWebResponse) request.GetResponse();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_permanent_redirect()
            {
                Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void redirect_is_cacheable()
            {
                Assert.Equal("max-age=31536000, public",_response.Headers[HttpResponseHeader.CacheControl]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header_that_is_to_stream_without_slash()
            {
                Assert.Equal(MakeUrlString(TestStream), _response.Headers[HttpResponseHeader.Location]);
            }

            public when_posting_an_event_as_array_to_stream_with_slash(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_deleting_to_stream_with_slash : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var request = CreateRequest(TestStream + "/", "", "DELETE", "application/json", null);
                request.AllowAutoRedirect = false;
                var response = (HttpWebResponse)request.GetResponse();
                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_permanent_redirect()
            {
                Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header_that_is_to_stream_without_slash()
            {
                Assert.Equal(MakeUrlString(TestStream), _response.Headers[HttpResponseHeader.Location]);
            }

            public when_deleting_to_stream_with_slash(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_getting_from_stream_with_slash : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var request = CreateRequest(TestStream + "/", "", "GET", "application/json", null);
                request.AllowAutoRedirect = false;
                var response = (HttpWebResponse)request.GetResponse();

                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_permanent_redirect()
            {
                Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header_that_is_to_stream_without_slash()
            {
                Assert.Equal(MakeUrlString(TestStream), _response.Headers[HttpResponseHeader.Location]);
            }

            public when_getting_from_stream_with_slash(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_getting_from_all_stream_with_slash : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var request = CreateRequest("/streams/$all/", "", "GET", "application/json", null);
                request.Credentials = DefaultData.AdminNetworkCredentials;
                request.AllowAutoRedirect = false;
                var response = (HttpWebResponse)request.GetResponse();

                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_permanent_redirect()
            {
                Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header_that_is_to_stream_without_slash()
            {
                Assert.Equal(MakeUrlString("/streams/$all"), _response.Headers[HttpResponseHeader.Location]);
            }

            public when_getting_from_all_stream_with_slash(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_getting_from_encoded_all_stream_with_slash : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var request = CreateRequest("/streams/%24all/", "", "GET", "application/json", null);
                request.Credentials = DefaultData.AdminNetworkCredentials;
                request.AllowAutoRedirect = false;
                var response = (HttpWebResponse)request.GetResponse();

                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            [Trait("Platform", "WIN")]
            public void returns_permanent_redirect()
            {
                Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            [Trait("Platform", "WIN")]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            [Trait("Platform", "WIN")]
            public void returns_a_location_header_that_is_to_stream_without_slash()
            {
                Assert.Equal(MakeUrl("/streams/$all").ToString(), _response.Headers[HttpResponseHeader.Location]);
            }

            public when_getting_from_encoded_all_stream_with_slash(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_as_array_to_metadata_stream_with_slash : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var request = CreateRequest(TestStream + "/metadata/", "", "POST", "application/json", null);
                request.AllowAutoRedirect = false;
                request.GetRequestStream()
                    .WriteJson(new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
                var response = (HttpWebResponse)request.GetResponse();

                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_permanent_redirect()
            {
                Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header_that_is_to_stream_without_slash()
            {
                Assert.Equal(MakeUrlString(TestStream + "/metadata"), _response.Headers[HttpResponseHeader.Location]);
            }

            public when_posting_an_event_as_array_to_metadata_stream_with_slash(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_getting_from_metadata_stream_with_slash : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var request = CreateRequest(TestStream + "/metadata/", "", "GET", "application/json", null);
                request.Credentials = DefaultData.AdminNetworkCredentials;
                request.AllowAutoRedirect = false;
                var response = (HttpWebResponse)request.GetResponse();

                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_permanent_redirect()
            {
                Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header_that_is_to_stream_without_slash()
            {
                Assert.Equal(MakeUrlString(TestStream + "/metadata"), _response.Headers[HttpResponseHeader.Location]);
            }

            public when_getting_from_metadata_stream_with_slash(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_without_EventId_as_array : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var response = MakeJsonPost(
                    TestStream,
                    new[] { new { EventType = "event-type", Data = new { A = "1" } } });

                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_bad_request_status_code()
            {
                Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
            }

            public when_posting_an_event_without_EventId_as_array(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_without_EventType_as_array : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                _response = MakeArrayEventsPost(
                    TestStream,
                    new[] { new { EventId = Guid.NewGuid(), Data = new { A = "1" } } });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_bad_request_status_code()
            {
                Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
            }

            public when_posting_an_event_without_EventType_as_array(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_event_with_date_time : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var response = MakeArrayEventsPost(
                    TestStream,
                    new[]
                    {
                        new
                        {
                            EventId = Guid.NewGuid(),
                            EventType = "event-type",
                            Data = new {A = "1987-11-07T00:00:00.000+01:00"}
                        },
                    });

                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_created_status_code()
            {
                Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void the_json_data_is_not_mangled()
            {
                var json = GetJson<JObject>(_response.Headers[HttpResponseHeader.Location]);
                HelperExtensions.AssertJson(new { A = "1987-11-07T00:00:00.000+01:00" }, json);
            }

            public when_posting_an_event_with_date_time(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_posting_an_events_as_array : HttpBehaviorSpecification
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                var response = MakeArrayEventsPost(
                    TestStream,
                    new[]
                        {
                            new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}},
                            new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "2"}},
                        });

                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_created_status_code()
            {
                Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header()
            {
                Assert.NotEmpty(_response.Headers[HttpResponseHeader.Location]);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header_for_the_first_posted_event()
            {
                var json = GetJson<JObject>(_response.Headers[HttpResponseHeader.Location]);
                HelperExtensions.AssertJson(new {A = "1"}, json);
            }

            public when_posting_an_events_as_array(SpecificationFixture data) : base(data)
            {
            }
        }

        public abstract class HttpBehaviorSpecificationWithSingleEvent : HttpBehaviorSpecification
        {
            protected HttpWebResponse _response;

            protected override void Given()
            {
                var response = MakeArrayEventsPost(
                    TestStream,
                    new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);

                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            public HttpBehaviorSpecificationWithSingleEvent(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_single_event_in_the_stream_as_atom_json: HttpBehaviorSpecificationWithSingleEvent
        {
            private JObject _json;

            protected override void When()
            {
                var json = GetJson<JObject>(TestStream + "/0", accept: ContentType.AtomJson);

                Fixture.AddStashedValueAssignment(this, instance => instance._json = json);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void request_succeeds()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_correct_body()
            {
                HelperExtensions.AssertJson(new { Content = new { Data = new {A = "1"}}}, _json);
            }

            public when_requesting_a_single_event_in_the_stream_as_atom_json(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_single_event_that_is_deleted_linkto : HttpSpecificationWithLinkToToDeletedEvents
        {
            protected override void When()
            {
                Get("/streams/" + LinkedStreamName + "/0", "", "application/json");
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void the_event_is_gone()
            {

                Assert.Equal(HttpStatusCode.NotFound, LastResponse.StatusCode);
            }

            public when_requesting_a_single_event_that_is_deleted_linkto(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_single_event_that_is_maxcount_deleted_linkto : SpecificationWithLinkToToMaxCountDeletedEvents
        {
            protected override void When()
            {
                Get("/streams/" + LinkedStreamName + "/0", "", "application/json");
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void the_event_is_gone()
            {

                Assert.Equal(HttpStatusCode.NotFound, LastResponse.StatusCode);
            }

            public when_requesting_a_single_event_that_is_maxcount_deleted_linkto(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_single_event_in_the_stream_as_event_json
            : HttpBehaviorSpecificationWithSingleEvent
        {
            private JObject _json;

            protected override void When()
            {
                var json = GetJson<JObject>(TestStream + "/0", accept: ContentType.EventJson);

                Fixture.AddStashedValueAssignment(this, instance => instance._json = json);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void request_succeeds()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_correct_body()
            {
                HelperExtensions.AssertJson(new {Data = new {A = "1"}}, _json);
            }

            public when_requesting_a_single_event_in_the_stream_as_event_json(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_single_event_in_the_stream_as_json: HttpBehaviorSpecificationWithSingleEvent
        {
            private JObject _json;

            protected override void When()
            {
                var json = GetJson<JObject>(TestStream + "/0", accept: ContentType.Json);

                Fixture.AddStashedValueAssignment(this, instance => instance._json = json);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void request_succeeds()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_correct_body()
            {
                HelperExtensions.AssertJson(new {A = "1"}, _json);
            }

            public when_requesting_a_single_event_in_the_stream_as_json(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_single_event_in_the_stream_as_atom_xml: HttpBehaviorSpecificationWithSingleEvent
        {
            //private JObject _json;

            protected override void When()
            {
                Get(TestStream + "/0", "", accept: ContentType.Atom);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void request_succeeds()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            public when_requesting_a_single_event_in_the_stream_as_atom_xml(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_single_event_in_the_stream_as_event_xml: HttpBehaviorSpecificationWithSingleEvent
        {
            //private JObject _json;

            protected override void When()
            {
                Get(TestStream + "/0", "", accept: ContentType.EventXml);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void request_succeeds()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            public when_requesting_a_single_event_in_the_stream_as_event_xml(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_single_event_in_the_stream_as_xml: HttpBehaviorSpecificationWithSingleEvent
        {
            //private JObject _json;

            protected override void When()
            {
                Get(TestStream + "/0", "", accept: ContentType.Xml);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void request_succeeds()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            public when_requesting_a_single_event_in_the_stream_as_xml(SpecificationFixture data) : base(data)
            {
            }
        }

    }
}
