using System;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.projections_manager;
using EventStore.Projections.Core.v8;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.v8
{
    public class when_running_a_faulting_v8_projection {

        
        public class when_event_handler_throws : TestFixtureWithJsProjection
        {
            protected override void Given()
            {
                _projection = @"
                    fromAll();
                    on_any(function(state, event) {
                        log(state.count);
                        throw ""failed"";
                        return state;
                    });
                ";
                _state = @"{""count"": 0}";
            }

            [Fact][Trait("Category", "v8")]
            public void process_event_throws_js1_exception()
            {
                string state;
                EmittedEventEnvelope[] emittedEvents;
                var thrown = Assert.Throws<Js1Exception>(() =>
                {
                    _stateHandler.ProcessEvent(
                        "", CheckpointTag.FromPosition(0, 10, 5), "stream1", "type1", "category", Guid.NewGuid(), 0, "metadata",
                        @"{""a"":""b""}", out state, out emittedEvents);
                });
                Assert.Equal("failed", thrown.Message);
            }
        }

        
        public class when_state_transform_throws : TestFixtureWithJsProjection
        {
            protected override void Given()
            {
                _projection = @"
                    fromAll().when({$any: function(state, event) {
                        return state;
                    }})
                    .transformBy(function (s) { throw ""failed"";});
                ";
                _state = @"{""count"": 0}";
            }

            [Fact][Trait("Category", "v8")]
            public void process_event_throws_js1_exception()
            {
                string state;
                EmittedEventEnvelope[] emittedEvents;
                _stateHandler.ProcessEvent(
                    "", CheckpointTag.FromPosition(0, 10, 5), "stream1", "type1", "category", Guid.NewGuid(), 0, "metadata",
                    @"{""a"":""b""}", out state, out emittedEvents);
                var thrown = Assert.Throws<Js1Exception>(() => { _stateHandler.TransformStateToResult(); });
                Assert.Equal("failed", thrown.Message);
            }
        }

    }
}
