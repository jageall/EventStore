using System;
using System.Net;
using System.Text.RegularExpressions;
using EventStore.Core.Tests.Http.Users.users;
using Xunit;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;
using EventStore.Transport.Http;

// ReSharper disable InconsistentNaming

namespace EventStore.Core.Tests.Http.PersistentSubscription
{
    public class when_acking_a_message : with_subscription_having_events
    {
        private HttpWebResponse _response;
        private string _ackLink;
        protected override void Given()
        {
            base.Given();
            var json = GetJson<JObject>(
               SubscriptionPath + "/1",
               ContentType.CompetingJson,
               _admin);
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            _ackLink = ((JObject)json)["entries"].Children().First()["links"].Children().First(x => x.Value<string>("relation") == "ack").Value<string>("uri");
        }

        protected override void When()
        {
            var response = MakePost(_ackLink, _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact]
        public void returns_accepted()
        {
            Assert.Equal(HttpStatusCode.Accepted, _response.StatusCode);
        }

        public when_acking_a_message(SpecificationFixture data) : base(data)
        {
        }
    }

    public class when_acking_messages : with_subscription_having_events
    {
        private HttpWebResponse _response;
        private string _ackAllLink;
        protected override void Given()
        {
            base.Given();
            var json = GetJson<JObject>(
               SubscriptionPath + "/" + Events.Count,
               ContentType.CompetingJson,
               _admin);
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            _ackAllLink = ((JObject)json)["links"].Children().First(x => x.Value<string>("relation") == "ackAll").Value<string>("uri");
        }

        protected override void When()
        {
            var response = MakePost(_ackAllLink, _admin);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._response = response;
            });
        }

        [Fact]
        public void returns_accepted()
        {
            Assert.Equal(HttpStatusCode.Accepted, _response.StatusCode);
        }

        public when_acking_messages(SpecificationFixture data) : base(data)
        {
        }
    }
}
