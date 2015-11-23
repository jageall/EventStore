using System;
using System.Collections.Generic;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.RequestManager.Managers;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Replication.TransactionCommit
{
    public class when_transaction_commit_gets_commit_timeout_before_final_commit : RequestManagerSpecification
    {
        protected override TwoPhaseRequestManagerBase OnManager(FakePublisher publisher)
        {
            return new TransactionCommitTwoPhaseRequestManager(publisher, 3, 3, PrepareTimeout, CommitTimeout);
        }

        protected override IEnumerable<Message> WithInitialMessages()
        {
            yield return new ClientMessage.TransactionCommit(InternalCorrId, ClientCorrId, Envelope, true, 4, null);
            yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.SingleWrite);
            yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.SingleWrite);
            yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.SingleWrite);
            yield return new StorageMessage.CommitAck(InternalCorrId, 3, 2, 3, 3);
        }

        protected override Message When()
        {
            return new StorageMessage.RequestManagerTimerTick(DateTime.UtcNow + CommitTimeout + TimeSpan.FromMinutes(5));
        }

        [Fact]
        public void failed_request_message_is_publised()
        {
            Assert.True(Produced.ContainsSingle<StorageMessage.RequestCompleted>(
                x => x.CorrelationId == InternalCorrId && x.Success == false));
        }

        [Fact]
        public void the_envelope_is_replied_to_with_failure()
        {
            Assert.True(Envelope.Replies.ContainsSingle<ClientMessage.TransactionCommitCompleted>(
                x => x.CorrelationId == ClientCorrId && x.Result == OperationResult.CommitTimeout));
        }
    }
}