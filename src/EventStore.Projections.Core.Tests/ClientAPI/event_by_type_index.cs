using System;
using Xunit;

namespace EventStore.Projections.Core.Tests.ClientAPI
{
    namespace event_by_type_index
    {

        public class with_existing_events : specification_with_standard_projections_runnning
        {
            protected override void Given()
            {
                base.Given();
                PostEvent("stream1", "type1", "{}");
                PostEvent("stream1", "type2", "{}");
                PostEvent("stream1", "type3", "{}");
                PostEvent("stream2", "type1", "{}");
                PostEvent("stream2", "type2", "{}");
                PostEvent("stream2", "type3", "{}");
            }


            public with_existing_events(SpecificationFixture fixture) : base(fixture)
            {
            }
        }

        
        public class when_creating : with_existing_events
        {
            protected override void When()
            {
                base.When();
                PostProjection(@"
fromAll().when({
    $init: function(){
        return {c: 0};
    },
    type1: count,
    type2: count
}).outputState()

function count(s,e) {
    return {c: s.c + 1};
}
");
            }

            [DebugBuildFact]
            [Trait("Category", "Network")]
            [Trait("Category", "ClientAPI")]
            public void result_is_correct()
            {
                AssertStreamTail("$projections-test-projection-result", "Result:{\"c\":4}");
            }

            public when_creating(SpecificationFixture fixture) : base(fixture)
            {
            }
        }

        
        public class when_posting_more_events : with_existing_events
        {
            protected override void When()
            {
                base.When();
                PostProjection(@"
fromAll().when({
    $init: function(){
        return {c: 0};
    },
    type1: count,
    type2: count
}).outputState()

function count(s,e) {
    return {c: s.c + 1};
}
");
                PostEvent("stream3", "type2", "{}");
                PostEvent("stream3", "type3", "{}");
                WaitIdle();
            }

            [DebugBuildFact]
            [Trait("Category", "Network")]
            [Trait("Category", "ClientAPI")]
            public void result_is_correct()
            {
                AssertStreamTail("$projections-test-projection-result", "Result:{\"c\":5}");
            }

            public when_posting_more_events(SpecificationFixture fixture) : base(fixture)
            {
            }
        }
    }
}
