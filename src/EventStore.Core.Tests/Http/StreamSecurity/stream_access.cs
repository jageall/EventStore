using System;
using System.Net;
using EventStore.ClientAPI;
using EventStore.Core.Services;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Tests.Http.Users;
using Xunit;
using Newtonsoft.Json.Linq;

namespace EventStore.Core.Tests.Http.StreamSecurity
{
    namespace stream_access
    {
        public class when_creating_a_secured_stream_by_posting_metadata: SpecificationWithUsers
        {
            private HttpWebResponse _response;

            protected override void When()
            {
                var metadata =
                    (StreamMetadata)
                    StreamMetadata.Build()
                                  .SetMetadataReadRole("admin")
                                  .SetMetadataWriteRole("admin")
                                  .SetReadRole("")
                                  .SetWriteRole("other");
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
                Fixture.AddStashedValueAssignment(this, instance => instance._response = response);
            }

            protected override MiniNode CreateMiniNode(string pathname)
            {
                return new MiniNode(pathname, enableTrustedAuth:true);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
            }

            [Fact]
            public void refuses_to_post_event_as_anonymous()
            {
                var response = PostEvent(new {Some = "Data"});
                Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void accepts_post_event_as_authorized_user()
            {
                var response = PostEvent(new {Some = "Data"}, GetCorrectCredentialsFor("user1"));
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void accepts_post_event_as_authorized_user_by_trusted_auth()
            {
                var uri = MakeUrl (TestStream);
                var request2 = WebRequest.Create (uri);
                var httpWebRequest = (HttpWebRequest)request2;
                httpWebRequest.ConnectionGroupName = TestStream;
                httpWebRequest.Method = "POST";
                httpWebRequest.ContentType = "application/vnd.eventstore.events+json";
                httpWebRequest.UseDefaultCredentials = false;
                httpWebRequest.Headers.Add("ES-TrustedAuth", "root; admin, other");
                httpWebRequest.GetRequestStream()
                              .WriteJson(
                                  new[]
                                      {
                                          new
                                              {
                                                  EventId = Guid.NewGuid(),
                                                  EventType = "event-type",
                                                  Data = new {Some = "Data"}
                                              }
                                      });
                var request = httpWebRequest;
                var httpWebResponse = GetRequestResponse(request);
                var response = httpWebResponse;
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }

            public when_creating_a_secured_stream_by_posting_metadata(SpecificationFixture data) : base(data)
            {
            }
        }
    }
}
