﻿using Xunit;

namespace EventStore.Projections.Core.Tests.ClientAPI.when_handling_deleted.with_from_all_foreach_projection
{
    
    public class when_running_and_events_are_indexed_but_tombstone : specification_with_standard_projections_runnning
    {
        protected override bool GivenStandardProjectionsRunning()
        {
            return false;
        }

        protected override void Given()
        {
            base.Given();
            PostEvent("stream-1", "type1", "{}");
            PostEvent("stream-1", "type2", "{}");
            PostEvent("stream-2", "type1", "{}");
            PostEvent("stream-2", "type2", "{}");
            WaitIdle();
            EnableStandardProjections();
            WaitIdle();
            DisableStandardProjections();
            WaitIdle();

            // required to flush index checkpoint
            {
                EnableStandardProjections();
                WaitIdle();
                DisableStandardProjections();
                WaitIdle();
            }


            HardDeleteStream("stream-1");
            WaitIdle();
        }

        protected override void When()
        {
            base.When();
            PostProjection(@"
fromAll().foreachStream().when({
    $init: function(){return {}},
    type1: function(s,e){s.a=1},
    type2: function(s,e){s.a=1},
    $deleted: function(s,e){s.deleted=1},
}).outputState();
");
            WaitIdle();
        }

        [DebugBuildFact]
        [Trait("Category", "Network")]
        [Trait("Category", "ClientAPI")]
        public void receives_deleted_notification()
        {
            AssertStreamTail("$projections-test-projection-stream-1-result", "Result:{\"deleted\":1}");
        }
    }
}
