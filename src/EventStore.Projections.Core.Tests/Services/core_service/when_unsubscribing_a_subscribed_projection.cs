using System;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_service
{
    
    public class when_unsubscribing_a_subscribed_projection : TestFixtureWithProjectionCoreService
    {
        private TestCoreProjection _committedeventHandler;
        private Guid _projectionCorrelationId;

        //private TestCoreProjection _committedeventHandler2;
        private Guid _projectionCorrelationId2;

        public when_unsubscribing_a_subscribed_projection()
        {
            _committedeventHandler = new TestCoreProjection();
            //_committedeventHandler2 = new TestCoreProjection();
            _projectionCorrelationId = Guid.NewGuid();
            _projectionCorrelationId2 = Guid.NewGuid();
            _readerService.Handle(
                new ReaderSubscriptionManagement.Subscribe(
                    _projectionCorrelationId, CheckpointTag.FromPosition(0, 0, 0), CreateReaderStrategy(),
                    new ReaderSubscriptionOptions(1000, 2000, false, stopAfterNEvents: null)));
            _readerService.Handle(
                new ReaderSubscriptionManagement.Subscribe(
                    _projectionCorrelationId2, CheckpointTag.FromPosition(0, 0, 0), CreateReaderStrategy(),
                    new ReaderSubscriptionOptions(1000, 2000, false, stopAfterNEvents: null)));
            // when
            _readerService.Handle(new ReaderSubscriptionManagement.Unsubscribe(_projectionCorrelationId));
        }

        [Fact]
        public void committed_events_are_no_longer_distributed_to_the_projection()
        {
            _readerService.Handle(
                new ReaderSubscriptionMessage.CommittedEventDistributed(_projectionCorrelationId, CreateEvent()));
            Assert.Equal(0, _committedeventHandler.HandledMessages.Count);
        }

        [Fact]
        public void the_projection_cannot_be_resumed()
        {
            //TODO JAG it is unclear which of these is under test, but I am assuming resume
            Assert.Throws<InvalidOperationException>(() =>_readerService.Handle(new ReaderSubscriptionManagement.Resume(_projectionCorrelationId)));
            //TODO JAG in which case these are handled in committed_events_are_no_longer_distributed_to_the_projection ...
            _readerService.Handle(
                new ReaderSubscriptionMessage.CommittedEventDistributed(_projectionCorrelationId, CreateEvent()));
            Assert.Equal(0, _committedeventHandler.HandledMessages.Count);
        }
    }
}
