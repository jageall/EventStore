using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.prepare_position_tagger
{
    
    public class when_creating_prepare_postion_tracker
    {
        private PositionTagger _tagger;
        private PositionTracker _positionTracker;

        public when_creating_prepare_postion_tracker()
        {
            _tagger = new PreparePositionTagger(0);
            _positionTracker = new PositionTracker(_tagger);
        }

        [Fact]
        public void it_can_be_updated()
        {
            // even not initialized (UpdateToZero can be removed)
            var newTag = CheckpointTag.FromPreparePosition(0, 50);
            _positionTracker.UpdateByCheckpointTagInitial(newTag);
        }

        [Fact]
        public void initial_position_cannot_be_set_twice()
        {
            var newTag = CheckpointTag.FromPreparePosition(0, 50);
            _positionTracker.UpdateByCheckpointTagInitial(newTag);
            Assert.Throws<InvalidOperationException>(() => { _positionTracker.UpdateByCheckpointTagInitial(newTag); });
        }

        [Fact]
        public void it_can_be_updated_to_zero()
        {
            _positionTracker.UpdateByCheckpointTagInitial(_tagger.MakeZeroCheckpointTag());
        }

        [Fact]
        public void it_cannot_be_updated_forward()
        {
            var newTag = CheckpointTag.FromPreparePosition(0, 50);
            Assert.Throws<InvalidOperationException>(() => { _positionTracker.UpdateByCheckpointTagForward(newTag); });
        }

    }
}
