using System;
using System.Text;
using System.Net;
using EventStore.Core.Tests.Helpers;
using EventStore.Transport.Http;
using Xunit;
using Newtonsoft.Json.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;

namespace EventStore.Core.Tests.Http.Streams
{
    namespace idempotency
    {
        public abstract class HttpBehaviorSpecificationOfSuccessfulCreateEvent : HttpBehaviorSpecification
        {
            protected HttpWebResponse _response;

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
            public void returns_a_location_header_ending_with_zero()
            {
                var location = _response.Headers[HttpResponseHeader.Location];
                var tail = location.Substring(location.Length - "/0".Length);
                Assert.Equal("/0", tail);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_a_location_header_that_can_be_read_as_json()
            {
                var json = GetJson<JObject>(_response.Headers[HttpResponseHeader.Location]);
                HelperExtensions.AssertJson(new { A = "1" }, json);
            }
        }

        public class when_posting_to_idempotent_guid_id_then_as_array : HttpBehaviorSpecificationOfSuccessfulCreateEvent
        {
            private Guid _eventId;

            protected override void Given()
            {
                var eventId = _eventId = Guid.NewGuid();
                PostEvent();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._eventId = eventId;
                });
            }

            protected override void When()
            {
                var response = MakeArrayEventsPost(
                    TestStream,
                    new[] { new { EventId = _eventId, EventType = "event-type", Data = new { A = "1" } } });
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            private void PostEvent() {
                var request = CreateRequest(TestStream + "/incoming/" + _eventId.ToString(), "", "POST", "application/json");
                request.Headers.Add("ES-EventType", "SomeType");
                request.AllowAutoRedirect = false;
                var data = "{a : \"1\"}";
                var bytes = Encoding.UTF8.GetBytes(data);
                request.ContentLength = data.Length;
                request.GetRequestStream().Write(bytes, 0, data.Length);
                _response = GetRequestResponse(request);
            }
        }

        public class when_posting_to_idempotent_guid_id_twice : HttpBehaviorSpecificationOfSuccessfulCreateEvent
        {
            private Guid _eventId;

            protected override void Given()
            {
                var eventId = _eventId = Guid.NewGuid();
                PostEvent();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._eventId = eventId;
                });
            }

            protected override void When()
            {
                PostEvent();
            }

            private void PostEvent() {
                var request = CreateRequest(TestStream + "/incoming/" + _eventId.ToString(), "", "POST", "application/json");
                request.Headers.Add("ES-EventType", "SomeType");
                request.AllowAutoRedirect = false;
                var data = "{a : \"1\"}";
                var bytes = Encoding.UTF8.GetBytes(data);
                request.ContentLength = data.Length;
                request.GetRequestStream().Write(bytes, 0, data.Length);
                var response = GetRequestResponse(request);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }
        }


        public class when_posting_to_idempotent_guid_id_three_times : HttpBehaviorSpecificationOfSuccessfulCreateEvent
        {
            private Guid _eventId;

            protected override void Given()
            {
                var eventId = _eventId = Guid.NewGuid();
                PostEvent();
                PostEvent();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._eventId = eventId;
                });
            }

            protected override void When()
            {
                PostEvent();
            }

            private void PostEvent() {
                var request = CreateRequest(TestStream + "/incoming/" + _eventId.ToString(), "", "POST", "application/json");
                request.Headers.Add("ES-EventType", "SomeType");
                request.AllowAutoRedirect = false;
                var data = "{a : \"1\"}";
                var bytes = Encoding.UTF8.GetBytes(data);
                request.ContentLength = data.Length;
                request.GetRequestStream().Write(bytes, 0, data.Length);
                var response = GetRequestResponse(request);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }
        }


        public class when_posting_an_event_once_raw_once_with_array : HttpBehaviorSpecificationOfSuccessfulCreateEvent
        {
            private Guid _eventId;

            protected override void Given()
            {
                var eventId = _eventId = Guid.NewGuid();
                PostEvent();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._eventId = eventId;
                });
            }

            protected override void When()
            {
                var response = MakeArrayEventsPost(
                    TestStream,
                    new[] { new { EventId = _eventId, EventType = "event-type", Data = new { A = "1" } } });
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            private void PostEvent() {
                var request = CreateRequest(TestStream, "", "POST", "application/json");
                request.Headers.Add("ES-EventId", _eventId.ToString());
                request.Headers.Add("ES-EventType", "SomeType");
                request.AllowAutoRedirect = false;
                var data = "{a : \"1\"}";
                var bytes = Encoding.UTF8.GetBytes(data);
                request.ContentLength = data.Length;
                request.GetRequestStream().Write(bytes, 0, data.Length);
                _response = GetRequestResponse(request);
            }
        }


        public class when_posting_an_event_twice_raw : HttpBehaviorSpecificationOfSuccessfulCreateEvent
        {
            private Guid _eventId;

            protected override void Given()
            {
                var eventId = _eventId = Guid.NewGuid();
                PostEvent();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._eventId = eventId;
                });
            }

            protected override void When()
            {
                PostEvent();
            }

            private void PostEvent() {
                var request = CreateRequest(TestStream, "", "POST", "application/json");
                request.Headers.Add("ES-EventId", _eventId.ToString());
                request.Headers.Add("ES-EventType", "SomeType");
                request.AllowAutoRedirect = false;
                var data = "{a : \"1\"}";
                var bytes = Encoding.UTF8.GetBytes(data);
                request.ContentLength = data.Length;
                request.GetRequestStream().Write(bytes, 0, data.Length);
                var response = GetRequestResponse(request);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

        }

        public class when_posting_an_event_three_times_raw : HttpBehaviorSpecificationOfSuccessfulCreateEvent
        {
            private Guid _eventId;

            protected override void Given()
            {
                var eventId = _eventId = Guid.NewGuid();
                PostEvent();
                PostEvent();
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._eventId = eventId;
                });
            }

            protected override void When()
            {
                PostEvent();
            }

            private void PostEvent() {
                var request = CreateRequest(TestStream, "", "POST", "application/json");
                request.Headers.Add("ES-EventId", _eventId.ToString());
                request.Headers.Add("ES-EventType", "SomeType");
                request.AllowAutoRedirect = false;
                var data = "{a : \"1\"}";
                var bytes = Encoding.UTF8.GetBytes(data);
                request.ContentLength = data.Length;
                request.GetRequestStream().Write(bytes, 0, data.Length);
                var response = GetRequestResponse(request);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

        }

        public class when_posting_an_event_twice_array : HttpBehaviorSpecificationOfSuccessfulCreateEvent
        {
            private Guid _eventId;

            protected override void Given()
            {
                var eventId = _eventId = Guid.NewGuid();
                var response1 = MakeArrayEventsPost(
                    TestStream,
                    new[] { new { EventId = _eventId, EventType = "event-type", Data = new { A = "1" } } });
                Assert.Equal(HttpStatusCode.Created, response1.StatusCode);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._eventId = eventId;
                });
            }

            protected override void When()
            {
                var response = MakeArrayEventsPost(
                    TestStream,
                    new[] { new { EventId = _eventId, EventType = "event-type", Data = new { A = "1" } } });
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

        }


        public class when_posting_an_event_three_times_as_array : HttpBehaviorSpecificationOfSuccessfulCreateEvent
        {
            private Guid _eventId;

            protected override void Given()
            {
                var eventId = _eventId = Guid.NewGuid();
                var response1 = MakeArrayEventsPost(
                    TestStream,
                    new[] { new { EventId = _eventId, EventType = "event-type", Data = new { A = "1" } } });
                Assert.Equal(HttpStatusCode.Created, response1.StatusCode);
                var response2 = MakeArrayEventsPost(
                    TestStream,
                    new[] { new { EventId = _eventId, EventType = "event-type", Data = new { A = "1" } } });
                Assert.Equal(HttpStatusCode.Created, response2.StatusCode);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._eventId = eventId;
                });
            }

            protected override void When()
            {
                var response = MakeArrayEventsPost(
                    TestStream,
                    new[] { new { EventId = _eventId, EventType = "event-type", Data = new { A = "1" } } });
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

        }

    }
}
