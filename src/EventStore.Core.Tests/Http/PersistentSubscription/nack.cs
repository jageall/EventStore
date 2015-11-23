using System;
using System.Net;
using System.Text.RegularExpressions;
using EventStore.Core.Tests.Http.Users.users;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;
using EventStore.Transport.Http;
using Xunit;

// ReSharper disable InconsistentNaming

namespace EventStore.Core.Tests.Http.PersistentSubscription
{
    public class when_nacking_a_message : with_subscription_having_events
    {
        private HttpWebResponse _response;
        private string _nackLink;
        protected override void Given()
        {
            base.Given();
            var json = GetJson<JObject>(
               SubscriptionPath + "/1",
               ContentType.CompetingJson,
               _admin);
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            _nackLink = ((JObject)json)["entries"].Children().First()["links"].Children().First(x => x.Value<string>("relation") == "nack").Value<string>("uri");
        }

        protected override void When()
        {
            _response = MakePost(_nackLink, _admin);
        }

        [Fact]
        public void returns_accepted()
        {
            Assert.Equal(HttpStatusCode.Accepted, _response.StatusCode);
        }
    }

    public class when_nacking_messages : with_subscription_having_events
    {
        private HttpWebResponse _response;
        private string _nackAllLink;
        protected override void Given()
        {
            base.Given();
            var json = GetJson<JObject>(
               SubscriptionPath + "/" + Events.Count,
               ContentType.CompetingJson,
               _admin);
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            _nackAllLink = ((JObject)json)["links"].Children().First(x => x.Value<string>("relation") == "nackAll").Value<string>("uri");
        }

        protected override void When()
        {
            _response = MakePost(_nackAllLink, _admin);
        }

        [Fact]
        public void returns_accepted()
        {
            Assert.Equal(HttpStatusCode.Accepted, _response.StatusCode);
        }
    }
}