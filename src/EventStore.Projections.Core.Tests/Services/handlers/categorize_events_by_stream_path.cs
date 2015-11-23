using System;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Standard;
using Xunit;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.handlers
{
    public static class categorize_events_by_stream_path
    {
        
        public class when_handling_simple_event
        {
            private CategorizeEventsByStreamPath _handler;
            private string _state;
            private EmittedEventEnvelope[] _emittedEvents;
            private bool _result;

            public when_handling_simple_event()
            {
                _handler = new CategorizeEventsByStreamPath("-", Console.WriteLine);
                _handler.Initialize();
                string sharedState;
                _result = _handler.ProcessEvent(
                    "", CheckpointTag.FromPosition(0, 200, 150), null,
                    new ResolvedEvent(
                        "cat1-stream1", 10, "cat1-stream1", 10, false, new TFPos(200, 150), Guid.NewGuid(),
                        "event_type", true, "{}", "{}"), out _state, out sharedState, out _emittedEvents);
            }

            [Fact]
            public void result_is_true()
            {
                Assert.True(_result);
            }

            [Fact]
            public void state_stays_null()
            {
                Assert.Null(_state);
            }

            [Fact]
            public void emits_correct_link()
            {
                Assert.NotNull(_emittedEvents);
                Assert.Equal(1, _emittedEvents.Length);
                var @event = _emittedEvents[0].Event;
                Assert.Equal("$>", @event.EventType);
                Assert.Equal("$ce-cat1", @event.StreamId);
                Assert.Equal("10@cat1-stream1", @event.Data);
            }

        }

        
        public class when_handling_link_to_event
        {
            private CategorizeEventsByStreamPath _handler;
            private string _state;
            private EmittedEventEnvelope[] _emittedEvents;
            private bool _result;

            public when_handling_link_to_event()
            {
                _handler = new CategorizeEventsByStreamPath("-", Console.WriteLine);
                _handler.Initialize();
                string sharedState;
                _result = _handler.ProcessEvent(
                    "", CheckpointTag.FromPosition(0, 200, 150), null,
                    new ResolvedEvent(
                        "cat2-stream2", 20, "cat2-stream2", 20, true, new TFPos(200, 150), Guid.NewGuid(),
                        "$>", true, "10@cat1-stream1", "{}"), out _state, out sharedState, out _emittedEvents);
            }

            [Fact]
            public void result_is_true()
            {
                Assert.True(_result);
            }

            [Fact]
            public void state_stays_null()
            {
                Assert.Null(_state);
            }

            [Fact]
            public void emits_correct_link()
            {
                Assert.NotNull(_emittedEvents);
                Assert.Equal(1, _emittedEvents.Length);
                var @event = _emittedEvents[0].Event;
                Assert.Equal("$>", @event.EventType);
                Assert.Equal("$ce-cat2", @event.StreamId);
                Assert.Equal("10@cat1-stream1", @event.Data);
            }

        }
    }
}
