using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.checkpoint_manager
{
    
    public class when_a_checkpoint_has_been_completed_and_requesting_checkpoint_to_stop :
        TestFixtureWithCoreProjectionCheckpointManager
    {
        private Exception _exception;

        protected override void Given()
        {
            AllWritesSucceed();
            base.Given();
            this._checkpointHandledThreshold = 2; //NOTE: does not play any role anymore here
        }

        protected override void When()
        {
            base.When();
            _exception = null;
            try
            {
                _checkpointReader.BeginLoadState();
                var checkpointLoaded =
                    _consumer.HandledMessages.OfType<CoreProjectionProcessingMessage.CheckpointLoaded>().First();
                _checkpointWriter.StartFrom(checkpointLoaded.CheckpointTag, checkpointLoaded.CheckpointEventNumber);
                _manager.BeginLoadPrerecordedEvents(checkpointLoaded.CheckpointTag);

                _manager.Start(CheckpointTag.FromStreamPosition(0, "stream", 10));
//                _manager.StateUpdated("", @"{""state"":""state1""}");
                _manager.EventProcessed(CheckpointTag.FromStreamPosition(0, "stream", 11), 77.7f);
//                _manager.StateUpdated("", @"{""state"":""state2""}");
                _manager.EventProcessed(CheckpointTag.FromStreamPosition(0, "stream", 12), 77.8f);
                _manager.CheckpointSuggested(CheckpointTag.FromStreamPosition(0, "stream", 12), 77.8f);
                _manager.Stopping();
            }
            catch (Exception ex)
            {
                _exception = ex;
            }
        }

        [Fact]
        public void does_not_throw()
        {
            Assert.Null(_exception);
        }

        [Fact]
        public void two_checkpoints_are_completed()
        {
            Assert.Equal(2, _projection._checkpointCompletedMessages.Count);
        }


        [Fact]
        public void only_one_checkpoint_has_been_written()
        {
            Assert.Equal(
                1,
                _consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
                    .ToStream("$projections-projection-checkpoint")
                    .Count());
        }
    }
}
