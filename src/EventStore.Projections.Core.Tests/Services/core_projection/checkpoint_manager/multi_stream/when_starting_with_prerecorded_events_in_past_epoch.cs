﻿using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.checkpoint_manager.multi_stream
{
    
    public class when_starting_with_prerecorded_events_in_past_epoch : TestFixtureWithMultiStreamCheckpointManager
    {
        private readonly CheckpointTag _tag1 =
            CheckpointTag.FromStreamPositions(0, new Dictionary<string, int> {{"a", 0}, {"b", 0}, {"c", 1}});

        private readonly CheckpointTag _tag2 =
            CheckpointTag.FromStreamPositions(0, new Dictionary<string, int> {{"a", 1}, {"b", 0}, {"c", 1}});

        //private readonly CheckpointTag _tag3 =CheckpointTag.FromStreamPositions(new Dictionary<string, int> {{"a", 1}, {"b", 1}, {"c", 1}});

        protected override void Given()
        {
            base.Given();
            _projectionVersion = new ProjectionVersion(1, 2, 2);
            ExistingEvent(
                "$projections-projection-checkpoint", "$ProjectionCheckpoint", @"{""v"":2, ""s"": {""a"": 0, ""b"": 0, ""c"": 0}}",
                "{}");
            ExistingEvent("a", "StreamCreated", "", "");
            ExistingEvent("b", "StreamCreated", "", "");
            ExistingEvent("c", "StreamCreated", "", "");

            ExistingEvent("a", "Event", "", @"{""data"":""a""");
            ExistingEvent("b", "Event", "", @"{""data"":""b""");
            ExistingEvent("c", "Event", "", @"{""data"":""c""");

            ExistingEvent("$projections-projection-order", "$>", @"{""v"":1, ""s"": {""a"": 0, ""b"": 0, ""c"": 0}}", "0@c");
            ExistingEvent("$projections-projection-order", "$>", @"{""v"":1, ""s"": {""a"": 0, ""b"": 0, ""c"": 1}}", "1@c");
            ExistingEvent("$projections-projection-order", "$>", @"{""v"":1, ""s"": {""a"": 1, ""b"": 0, ""c"": 1}}", "1@a");
            ExistingEvent("$projections-projection-order", "$>", @"{""v"":1, ""s"": {""a"": 1, ""b"": 1, ""c"": 1}}", "1@b");
            ExistingEvent("$projections-projection-order", "$>", @"{""v"":2, ""s"": {""a"": 0, ""b"": 0, ""c"": 0}}", "0@c");
            ExistingEvent("$projections-projection-order", "$>", @"{""v"":2, ""s"": {""a"": 0, ""b"": 0, ""c"": 1}}", "1@c");
            ExistingEvent("$projections-projection-order", "$>", @"{""v"":2, ""s"": {""a"": 1, ""b"": 0, ""c"": 1}}", "1@a");
        }

        protected override void When()
        {
            base.When();
            _checkpointReader.BeginLoadState();
            var checkpointLoaded =
                _consumer.HandledMessages.OfType<CoreProjectionProcessingMessage.CheckpointLoaded>().First();
            _checkpointWriter.StartFrom(checkpointLoaded.CheckpointTag, checkpointLoaded.CheckpointEventNumber);
            _manager.BeginLoadPrerecordedEvents(checkpointLoaded.CheckpointTag);

        }

        [Fact]
        public void sends_correct_checkpoint_loaded_message()
        {
            Assert.Equal(1, _projection._checkpointLoadedMessages.Count);
            Assert.Equal(
                CheckpointTag.FromStreamPositions(0, new Dictionary<string, int> {{"a", 0}, {"b", 0}, {"c", 0}}),
                _projection._checkpointLoadedMessages.Single().CheckpointTag);
            Assert.Equal("{}", _projection._checkpointLoadedMessages.Single().CheckpointData);
        }

        [Fact]
        public void sends_correct_prerecorded_events_loaded_message()
        {
            Assert.Equal(1, _projection._prerecordedEventsLoadedMessages.Count);
            Assert.Equal(
                CheckpointTag.FromStreamPositions(0, new Dictionary<string, int> {{"a", 1}, {"b", 0}, {"c", 1}}),
                _projection._prerecordedEventsLoadedMessages.Single().CheckpointTag);
        }

        [Fact]
        public void sends_committed_event_received_messages_in_correct_order()
        {
            var messages = HandledMessages.OfType<EventReaderSubscriptionMessage.CommittedEventReceived>().ToList();
            Assert.Equal(2, messages.Count);

            var message1 = messages[0];
            var message2 = messages[1];

            Assert.Equal(_tag1, message1.CheckpointTag);
            Assert.Equal(_tag2, message2.CheckpointTag);
        }

    }
}
