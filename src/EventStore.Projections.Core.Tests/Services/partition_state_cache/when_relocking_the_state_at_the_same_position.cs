using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.partition_state_cache
{
    
    public class when_relocking_the_state_at_the_same_position
    {
        private PartitionStateCache _cache;
        private CheckpointTag _cachedAtCheckpointTag;

        public when_relocking_the_state_at_the_same_position()
        {
            //given
            _cache = new PartitionStateCache();
            _cachedAtCheckpointTag = CheckpointTag.FromPosition(0, 1000, 900);
            _cache.CacheAndLockPartitionState("partition", new PartitionState("data", null, _cachedAtCheckpointTag), _cachedAtCheckpointTag);
        }
        
        [Fact]
        public void thorws_invalid_operation_exception_if_not_allowed()
        {
            Assert.Throws<InvalidOperationException>(
                () => { _cache.TryGetAndLockPartitionState("partition", CheckpointTag.FromPosition(0, 1000, 900)); });
        }

        [Fact]
        public void the_state_can_be_retrieved()
        {
            var state = _cache.TryGetPartitionState("partition");
            Assert.Equal("data", state.State);
        }

    }
}
