using System;
using System.Collections.Generic;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.multistream_position_tagger
{
    
    public class when_creating_multistream_postion_tracker
    {
        private MultiStreamPositionTagger _tagger;
        private PositionTracker _positionTracker;

        public when_creating_multistream_postion_tracker()
        {
            _tagger = new MultiStreamPositionTagger(0, new []{"stream1", "stream2"});
            _positionTracker = new PositionTracker(_tagger);
        }

        [Fact]
        public void it_can_be_updated_with_correct_streams()
        {
            // even not initialized (UpdateToZero can be removed)
            var newTag = CheckpointTag.FromStreamPositions(0, new Dictionary<string, int>{{"stream1", 10}, {"stream2", 20}});
            _positionTracker.UpdateByCheckpointTagInitial(newTag);
        }
        
        [Fact]
        public void it_cannot_be_updated_with_other_streams()
        {
            var newTag = CheckpointTag.FromStreamPositions(0, new Dictionary<string, int> { { "stream1", 10 }, { "stream3", 20 } });
            Assert.Throws<InvalidOperationException>(() => { _positionTracker.UpdateByCheckpointTagInitial(newTag); });
        }
        
        [Fact]
        public void it_cannot_be_updated_forward()
        {
            var newTag = CheckpointTag.FromStreamPositions(0, new Dictionary<string, int> { { "stream1", 10 }, { "stream2", 20 } });
            Assert.Throws<InvalidOperationException>(() => { _positionTracker.UpdateByCheckpointTagForward(newTag); });
        }
        
        [Fact]
        public void initial_position_cannot_be_set_twice()
        {
            var newTag = CheckpointTag.FromStreamPositions(0, new Dictionary<string, int> { { "stream1", 10 }, { "stream2", 20 } });
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
