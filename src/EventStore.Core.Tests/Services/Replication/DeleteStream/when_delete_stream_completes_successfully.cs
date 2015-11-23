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
    public class when_delete_stream_completes_successfully : RequestManagerSpecification
    {
        protected override TwoPhaseRequestManagerBase OnManager(FakePublisher publisher)
        {
            return new DeleteStreamTwoPhaseRequestManager(publisher, 3, 3, PrepareTimeout, CommitTimeout);
        }

        protected override IEnumerable<Message> WithInitialMessages()
        {
            yield return new ClientMessage.DeleteStream(InternalCorrId, ClientCorrId, Envelope, true, "test123", ExpectedVersion.Any, true, null);
//            yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.StreamDelete);
//            yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.StreamDelete);
//            yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.StreamDelete);
            yield return new StorageMessage.CommitAck(InternalCorrId, 100, 2, 3, 3);
            yield return new StorageMessage.CommitAck(InternalCorrId, 100, 2, 3, 3);

        }

        protected override Message When()
        {
            return new StorageMessage.CommitAck(InternalCorrId, 100, 2, 3, 3);
        }

        [Fact]
        public void successful_request_message_is_publised()
        {
            Assert.True(Produced.ContainsSingle<StorageMessage.RequestCompleted>(
                x => x.CorrelationId == InternalCorrId && x.Success));
        }

        [Fact]
        public void the_envelope_is_replied_to_with_success()
        {
            Assert.True(Envelope.Replies.ContainsSingle<ClientMessage.DeleteStreamCompleted>(
                x => x.CorrelationId == ClientCorrId && x.Result == OperationResult.Success));
        }
    }
}