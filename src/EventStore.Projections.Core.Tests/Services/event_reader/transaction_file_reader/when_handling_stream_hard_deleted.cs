using System;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Core.Tests.Services.TimeService;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.event_reader.transaction_file_reader
{
    
    public class when_handling_stream_hard_deleted : TestFixtureWithExistingEvents
    {
        private TransactionFileEventReader _edp;
        private Guid _distibutionPointCorrelationId;
        private Guid _firstEventId;
        private Guid _secondEventId;

        protected override void Given()
        {
            TicksAreHandledImmediately();
        }

        private FakeTimeProvider _fakeTimeProvider;

        public when_handling_stream_hard_deleted()
        {
            _distibutionPointCorrelationId = Guid.NewGuid();
            _fakeTimeProvider = new FakeTimeProvider();
            _edp = new TransactionFileEventReader(_bus, _distibutionPointCorrelationId, null, new TFPos(100, 50), _fakeTimeProvider,
                deliverEndOfTFPosition: false);
            _edp.Resume();
            _firstEventId = Guid.NewGuid();
            _secondEventId = Guid.NewGuid();
            _edp.Handle(
                new ClientMessage.ReadAllEventsForwardCompleted(
                    _distibutionPointCorrelationId, ReadAllResult.Success, null,
                    new[]
                    {
                        ResolvedEvent.ForUnresolvedEvent(
                            new EventRecord(
                                1, 50, Guid.NewGuid(), _firstEventId, 50, 0, "a", ExpectedVersion.Any,
                                _fakeTimeProvider.Now,
                                PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
                                "event_type1", new byte[] {1}, new byte[] {2}), 100),
                        ResolvedEvent.ForUnresolvedEvent(
                            new EventRecord(
                                2, 150, Guid.NewGuid(), _secondEventId, 150, 0, "a", ExpectedVersion.Any,
                                _fakeTimeProvider.Now,
                                PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
                                SystemEventTypes.StreamDeleted, new byte[] {1}, new byte[] {2}), 200),
                    }, null, false, 100,
                    new TFPos(200, 150), new TFPos(500, -1), new TFPos(100, 50), 500));

        }

        [Fact]
        public void publishes_event_reader_partition_deleted_messages()
        {
            var deleteds =
                _consumer.HandledMessages.OfType<ReaderSubscriptionMessage.EventReaderPartitionDeleted>().ToArray();
            Assert.Equal(1, deleteds.Count());
            Assert.Equal("a", deleteds[0].Partition);
        }
    }
}
