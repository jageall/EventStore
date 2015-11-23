using System;
using System.Net;
using System.Text.RegularExpressions;
using EventStore.Core.Tests.Http.Users.users;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;
using EventStore.Transport.Http;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common;
using EventStore.Core.Data;
using Xunit;
using ResolvedEvent = EventStore.ClientAPI.ResolvedEvent;

namespace EventStore.Core.Tests.Http.PersistentSubscription
{
    public class when_parking_a_message : with_subscription_having_events
    {
        private string _nackLink;
        private Guid _eventIdToPark;
        private RecordedEvent _parkedEvent;
        private AutoResetEvent _eventParked;
        protected override void Given()
        {
            NumberOfEventsToCreate = 1;
            base.Given();
            var json = GetJson<JObject>(
               SubscriptionPath + "/1?embed=rich",
               ContentType.CompetingJson,
               _admin);
            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            var entries = json != null ? json["entries"].ToList() : new List<JToken>();
            _nackLink = entries[0]["links"][3]["uri"].ToString() + "?action=park";
            var eventIdToPark = Guid.Parse(entries[0]["eventId"].ToString());
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._eventIdToPark = eventIdToPark;
            });
        }

        protected override void When()
        {
            var parkedStreamId = String.Format("$persistentsubscription-{0}::{1}-parked", TestStreamName, GroupName);
            var eventParked = new AutoResetEvent(false);
            Connection.SubscribeToStreamAsync(parkedStreamId, true, (x, y) =>
            {
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._parkedEvent = y.Event;    
                });
                Fixture.AssignStashedValues(this);
                eventParked.Set();
            }, 
            (x,y,z)=> { }, 
            DefaultData.AdminCredentials).Wait();

            var response = MakePost(_nackLink, _admin);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            Fixture.AddStashedValueAssignment(this, instance =>
            {

                instance._eventParked = eventParked;
            });
        }

        [Fact]
        public void should_have_parked_the_event()
        {
            Assert.True(_eventParked.WaitOne(TimeSpan.FromSeconds(5)));
            Assert.NotNull(_parkedEvent);
            Assert.Equal(_eventIdToPark, _parkedEvent.EventId);
        }
    }

    public class when_replaying_parked_message : with_subscription_having_events
    {
        private AutoResetEvent _eventParked;
        private Guid _eventIdToPark;
        private EventStore.ClientAPI.ResolvedEvent _replayedParkedEvent;
        protected override void Given()
        {
            NumberOfEventsToCreate = 1;
            base.Given();

            var json = GetJson<JObject>(
               SubscriptionPath + "/1?embed=rich",
               ContentType.CompetingJson,
               _admin);

            Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);

            var _entries = json != null ? json["entries"].ToList() : new List<JToken>();
            var nackLink = _entries[0]["links"][3]["uri"].ToString() + "?action=park";
            var eventIdToPark = Guid.Parse(_entries[0]["eventId"].ToString());

            //Park the message
            var response = MakePost(nackLink, _admin);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._eventIdToPark = eventIdToPark;
            });
        }

        protected override void When()
        {
            var eventParked = new AutoResetEvent(false);
            Connection.ConnectToPersistentSubscription(TestStreamName, GroupName, (x, y) =>
            {
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._replayedParkedEvent = y;
                });
                Fixture.AssignStashedValues(this);
                eventParked.Set();
            },
            (x, y, z) => { },
            DefaultData.AdminCredentials);

            //Replayed parked messages
            var response = MakePost(SubscriptionPath + "/replayParked", _admin);

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._eventParked = eventParked;
            });
        }

        [Fact]
        public void should_have_replayed_the_parked_event()
        {
            Assert.True(_eventParked.WaitOne(TimeSpan.FromSeconds(5)));
            Assert.Equal(_replayedParkedEvent.Event.EventId, _eventIdToPark);
        }
    }
}
