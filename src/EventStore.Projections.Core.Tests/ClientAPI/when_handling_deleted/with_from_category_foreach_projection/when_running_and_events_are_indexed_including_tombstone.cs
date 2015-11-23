using Xunit;

namespace EventStore.Projections.Core.Tests.ClientAPI.when_handling_deleted.with_from_category_foreach_projection
{
    
    public class when_running_and_events_are_indexed_including_tombstone :
        specification_with_standard_projections_runnning
    {
        protected override bool GivenStandardProjectionsRunning()
        {
            return false;
        }

        protected override void Given()
        {
            base.Given();
            PostEvent("stream-1", "type1", "{}");
            PostEvent("stream-2", "type1", "{}");
            PostEvent("stream-2", "type2", "{}");
            PostEvent("stream-1", "type2", "{}");
            HardDeleteStream("stream-1");
            WaitIdle();
            EnableStandardProjections();
            WaitIdle();
        }

        protected override void When()
        {
            base.When();

            PostProjection(@"
fromCategory('stream').foreachStream().when({
    $init: function(){return {a:0}},
    type1: function(s,e){s.a++},
    type2: function(s,e){s.a++},
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
            DumpStream("$ce-stream");
            AssertStreamTail("$projections-test-projection-stream-1-result", "Result:{\"a\":0,\"deleted\":1}");
        }
    }
}
