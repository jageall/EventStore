using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace EventStore.Atom.Tests
{
    public class BasicTests : FixtureSetup
    {
        [Test]
        public void CanReadEventBack()
        {
            var result = AppendEvents("test", new TestEvent(Guid.NewGuid(), "test", new {Foo = "bar"})).Result;
            Assert.True(result.IsSuccessStatusCode, result.StatusCode.ToString());
            result = Client.GetAsync("streams/test").Result;
            Assert.True(result.IsSuccessStatusCode, result.StatusCode.ToString());
        }
    }
    [TestFixture]
    public abstract class FixtureSetup
    {
        private HttpClient _client;
        private MiniNode _miniNode;

        public HttpClient Client
        {
            get { return _client; }
        }

        [TestFixtureSetUp]
        public void Setup()
        {
            var url = ConfigurationManager.AppSettings.Get("url");
            if (string.IsNullOrEmpty(url))
            {
                _miniNode = new MiniNode("foo");
                _miniNode.Start();
                url = new UriBuilder("http", _miniNode.HttpEndPoint.Address.ToString(), _miniNode.HttpEndPoint.Port).Uri.AbsoluteUri;
            }
            var uri = new Uri(url);
            _client = new HttpClient { BaseAddress = uri };
            
        }

        [TestFixtureTearDown]
        public void Teardown()
        {
            if(_client != null) _client.Dispose();
            if(_miniNode != null) _miniNode.Shutdown();
        }

        protected Task<HttpResponseMessage> AppendEvents(string stream, TestEvent first, params TestEvent[] others)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, "streams/"+stream);
            var jsonEvents = new JArray {ConvertToJson(first)};
            foreach (var data in others)
            {
                jsonEvents.Add(ConvertToJson(data));
            }
            request.Content = new StringContent(jsonEvents.ToString(),Encoding.UTF8,"application/vnd.eventstore.events+json");
            return _client.SendAsync(request);
        }

        private static JObject ConvertToJson(TestEvent data)
        {
            return new JObject(
                new JProperty("eventId", data.Id.ToString("D")),
                new JProperty("eventType", data.Type),
                new JProperty("data", JObject .FromObject(data.Event)),
                new JProperty("metadata", data.Metadata != null ? JObject.FromObject(data.Metadata): null)
                );
        }

        protected class TestEvent
        {
            public TestEvent(Guid id, string type, object @event, object metadata = null)
            {
                Id = id;
                Type = type;
                Event = @event;
                Metadata = metadata;
            }

            public Guid Id { get; private set; }
            public string Type { get; private set; }
            public object Event { get; private set; }
            public object Metadata { get; private set; }
        }
    }
}
