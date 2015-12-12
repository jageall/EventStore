using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.stream_position_tagger
{
    
    public class when_creating_stream_postion_tracker
    {
        private StreamPositionTagger _tagger;
        private PositionTracker _positionTracker;

        public when_creating_stream_postion_tracker()
        {
            _tagger = new StreamPositionTagger(0, "stream1");
            _positionTracker = new PositionTracker(_tagger);
        }

        [Fact]
        public void it_can_be_updated_with_correct_stream()
        {
            // even not initialized (UpdateToZero can be removed)
            var newTag = CheckpointTag.FromStreamPosition(0, "stream1", 1);
            _positionTracker.UpdateByCheckpointTagInitial(newTag);
        }
        
        [Fact]
        public void it_cannot_be_updated_with_other_stream()
        {
            var newTag = CheckpointTag.FromStreamPosition(0, "other_stream1", 1);
            Assert.Throws<InvalidOperationException>(() => { _positionTracker.UpdateByCheckpointTagInitial(newTag); });
        }
        
        [Fact]
        public void it_cannot_be_updated_forward()
        {
            var newTag = CheckpointTag.FromStreamPosition(0, "stream1", 1);
            Assert.Throws<InvalidOperationException>(() => { _positionTracker.UpdateByCheckpointTagForward(newTag); });
        }
        
        [Fact]
        public void initial_position_cannot_be_set_twice()
        {
            var newTag = CheckpointTag.FromStreamPosition(0, "stream1", 1);
            _positionTracker.UpdateByCheckpointTagInitial(newTag);
            Assert.Throws<InvalidOperationException>(() => { _positionTracker.UpdateByCheckpointTagInitial(newTag); });
        }

        [Fact]
        public void it_can_be_updated_to_zero()
        {
            _positionTracker.UpdateByCheckpointTagInitial(_tagger.MakeZeroCheckpointTag());
        }
    }
}
