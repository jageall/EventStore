using System;
using System.Net;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Tests.Http.Streams.basic;
using Newtonsoft.Json.Linq;
using Xunit;

namespace EventStore.Core.Tests.Http.Streams
{
    public class when_posting_metadata_as_json_to_non_existing_stream : HttpBehaviorSpecification
    {
        private HttpWebResponse _response;

        protected override void Given()
        {
        }

        protected override void When()
        {
            var req = CreateRawJsonPostRequest(TestStream + "/metadata", "POST", new {A = "1"},
                DefaultData.AdminNetworkCredentials);
            req.Headers.Add("ES-EventId", Guid.NewGuid().ToString());
            var response = (HttpWebResponse) req.GetResponse();
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
            HelperExtensions.AssertJson(new { A = "1" }, json);
        }

        public when_posting_metadata_as_json_to_non_existing_stream(SpecificationFixture data) : base(data)
        {
        }
    }

    public class when_posting_metadata_as_json_to_existing_stream : HttpBehaviorSpecificationWithSingleEvent
    {
        protected override void Given()
        {
            var response = MakeArrayEventsPost(
                TestStream,
                new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        protected override void When()
        {
            var req = CreateRawJsonPostRequest(TestStream + "/metadata", "POST", new { A = "1" },
                DefaultData.AdminNetworkCredentials);
            req.Headers.Add("ES-EventId", Guid.NewGuid().ToString());
            var response = (HttpWebResponse)req.GetResponse();
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
            HelperExtensions.AssertJson(new { A = "1" }, json);
        }

        public when_posting_metadata_as_json_to_existing_stream(SpecificationFixture data) : base(data)
        {
        }
    }

    public class when_getting_metadata_for_an_existing_stream_and_no_metadata_exists : HttpBehaviorSpecificationWithSingleEvent
    {
        protected override void Given()
        {
            _response = MakeArrayEventsPost(
                TestStream,
                new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
        }

        protected override void When()
        {
            Get(TestStream + "/metadata", String.Empty, EventStore.Transport.Http.ContentType.Json, DefaultData.AdminNetworkCredentials);
        }

        [Fact]
        public void returns_ok_status_code()
        {
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
        }

        [Fact]
        public void returns_empty_etag()
        {
            Assert.True(string.IsNullOrEmpty(LastResponse.Headers["ETag"]), LastResponse.Headers["ETag"]);
        }

        [Fact]
        public void returns_empty_body()
        {
            Assert.Equal("{}", LastResponseBody);
        }

        public when_getting_metadata_for_an_existing_stream_and_no_metadata_exists(SpecificationFixture data) : base(data)
        {
        }
    }
}