using System;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Services.TimerService;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using ReadStreamResult = EventStore.Core.Data.ReadStreamResult;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.event_reader.stream_reader
{
    
    public class when_paused_then_handling_no_stream : TestFixtureWithExistingEvents
    {
        private StreamEventReader _edp;
        private Guid _distibutionPointCorrelationId;
        
        protected override void Given()
        {
            TicksAreHandledImmediately();
        }

        public when_paused_then_handling_no_stream()
        {
            _distibutionPointCorrelationId = Guid.NewGuid();
            _edp = new StreamEventReader(_bus, _distibutionPointCorrelationId, null, "stream", 0, new RealTimeProvider(), false,
                produceStreamDeletes: false);
            _edp.Resume();
            _edp.Pause();
            _edp.Handle(
                new ClientMessage.ReadStreamEventsForwardCompleted(
                    _distibutionPointCorrelationId, "stream", 100, 100, ReadStreamResult.NoStream, new ResolvedEvent[0]
                    , null, false, "", -1, ExpectedVersion.NoStream, true, 200));
        }

        [Fact]
        public void can_be_resumed()
        {
            _edp.Resume();
        }
        
        [Fact]
        public void cannot_be_paused()
        {
            Assert.Throws<InvalidOperationException>(() => { _edp.Pause(); });
        }

        [Fact]
        public void publishes_read_events_from_beginning_with_correct_next_event_number()
        {
            Assert.Equal(1, _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Count());
            Assert.Equal(
                "stream", _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last().EventStreamId);
            Assert.Equal(
                0, _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last().FromEventNumber);
        }

        [Fact]
        public void publishes_correct_committed_event_received_messages()
        {
            Assert.Equal(
                1, _consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().Count());
            var first =
                _consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().Single();

            Assert.Null(first.Data);
            Assert.Equal(200, first.SafeTransactionFileReaderJoinPosition);
        }

        [Fact]
        public void does_not_publish_schedule()
        {
            Assert.Equal(0, _consumer.HandledMessages.OfType<TimerMessage.Schedule>().Count());
        }

    }
}
