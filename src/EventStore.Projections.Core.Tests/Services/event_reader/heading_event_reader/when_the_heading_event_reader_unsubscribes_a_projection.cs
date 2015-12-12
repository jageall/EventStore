using System;
using EventStore.Core.Data;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Tests.Helpers;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.projections_manager.managed_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_reader.heading_event_reader
{
    public class when_the_heading_event_reader_unsubscribes_a_projection : TestFixtureWithReadWriteDispatchers
    {
        private HeadingEventReader _point;
        private Guid _distibutionPointCorrelationId;
        private FakeReaderSubscription _subscription;
        private Guid _projectionSubscriptionId;

        public when_the_heading_event_reader_unsubscribes_a_projection()
        {
            _point = new HeadingEventReader(10);
            _distibutionPointCorrelationId = Guid.NewGuid();
            _point.Start(
                _distibutionPointCorrelationId,
                new TransactionFileEventReader(_bus, _distibutionPointCorrelationId, null, new TFPos(0, -1), new RealTimeProvider()));
            _point.Handle(
                ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
                    _distibutionPointCorrelationId, new TFPos(20, 10), "stream", 10, false, Guid.NewGuid(),
                    "type", false, new byte[0], new byte[0]));
            _point.Handle(
                ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
                    _distibutionPointCorrelationId, new TFPos(40, 30), "stream", 11, false, Guid.NewGuid(),
                    "type", false, new byte[0], new byte[0]));
            _subscription = new FakeReaderSubscription();
            _projectionSubscriptionId = Guid.NewGuid();
            var subscribed = _point.TrySubscribe(_projectionSubscriptionId, _subscription, 30);
            Assert.True(subscribed); // ensure we really unsubscribing.. even if it is tested elsewhere
            _point.Unsubscribe(_projectionSubscriptionId);
        }


        [Fact]
        public void projection_does_not_receive_any_events_after_unsubscribing()
        {
            var count = _subscription.ReceivedEvents.Count;
            _point.Handle(
                ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
                    _distibutionPointCorrelationId, new TFPos(60, 50), "stream", 12, false, Guid.NewGuid(),
                    "type", false, new byte[0], new byte[0]));
            Assert.Equal(count, _subscription.ReceivedEvents.Count);
        }

        [Fact]
        public void it_cannot_be_unsubscribed_twice()
        {
            Assert.Throws<InvalidOperationException>(() =>
                _point.Unsubscribe(_projectionSubscriptionId));
        }

        [Fact]
        public void projection_can_resubscribe_with()
        {
            var subscribed = _point.TrySubscribe(_projectionSubscriptionId, _subscription, 30);
            Assert.Equal(true, subscribed);
        }
    }
}
