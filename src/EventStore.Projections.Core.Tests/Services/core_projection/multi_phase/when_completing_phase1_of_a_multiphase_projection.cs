﻿using System.Linq;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.multi_phase
{
    
    public class when_completing_phase1_of_a_multiphase_projection : specification_with_multi_phase_core_projection
    {
        protected override void When()
        {
            _coreProjection.Start();
            Phase1.Complete();
        }

        [Fact]
        public void stops_phase1_checkpoint_manager()
        {
            Assert.True(Phase1CheckpointManager.Stopped_);
        }

        [Fact]
        public void initializes_phase2()
        {
            Assert.True(Phase2.InitializedFromCheckpoint);
        }

        [Fact]
        public void updates_checkpoint_tag_phase()
        {
            Assert.Equal(1, _coreProjection.LastProcessedEventPosition.Phase);
        }

        [Fact]
        public void publishes_subscribe_message()
        {
            Assert.Equal(1, Phase2.SubscribeInvoked);
        }
    }
}
