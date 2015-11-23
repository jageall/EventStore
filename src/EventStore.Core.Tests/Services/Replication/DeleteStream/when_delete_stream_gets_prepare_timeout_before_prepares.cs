using System;
using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.RequestManager.Managers;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Services.Replication.DeleteStream
{
    public class when_delete_stream_gets_prepare_timeout_before_prepares : RequestManagerSpecification
    {
        protected override TwoPhaseRequestManagerBase OnManager(FakePublisher publisher)
        {
            return new DeleteStreamTwoPhaseRequestManager(publisher, 3, 3, PrepareTimeout, CommitTimeout);
        }

        protected override IEnumerable<Message> WithInitialMessages()
        {
            yield return new ClientMessage.DeleteStream(InternalCorrId, ClientCorrId, Envelope, true, "test123", ExpectedVersion.Any, true, null);
        }

        protected override Message When()
        {
            return new StorageMessage.RequestManagerTimerTick(DateTime.UtcNow + PrepareTimeout + TimeSpan.FromMinutes(5));
        }

        [Fact(Skip = "DeleteStream operation is not 2-phase now, it does not expect PrepareAck anymore.")]
        public void failed_request_message_is_published()
        {
            Assert.True(Produced.ContainsSingle<StorageMessage.RequestCompleted>(
                x => x.CorrelationId == InternalCorrId && x.Success == false));
        }

        [Fact(Skip = "DeleteStream operation is not 2-phase now, it does not expect PrepareAck anymore.")]
        public void the_envelope_is_replied_to_with_failure()
        {
            Assert.True(Envelope.Replies.ContainsSingle<ClientMessage.DeleteStreamCompleted>(
                x => x.CorrelationId == ClientCorrId && x.Result == OperationResult.PrepareTimeout));
        }
    }
}