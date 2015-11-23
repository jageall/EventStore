using System;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection
{
    
    public class when_loading_an_existing_projection : TestFixtureWithCoreProjectionLoaded
    {
        private string _testProjectionState = @"{""test"":1}";

        protected override void Given()
        {
            ExistingEvent(
                "$projections-projection-result", "Result",
                @"{""c"": 100, ""p"": 50}", _testProjectionState);
            ExistingEvent(
                "$projections-projection-checkpoint", "$ProjectionCheckpoint",
                @"{""c"": 100, ""p"": 50}", _testProjectionState);
            ExistingEvent(
                "$projections-projection-result", "Result",
                @"{""c"": 200, ""p"": 150}", _testProjectionState);
            ExistingEvent(
                "$projections-projection-result", "Result",
                @"{""c"": 300, ""p"": 250}", _testProjectionState);
        }

        protected override void When()
        {
        }


        [Fact]
        public void should_not_subscribe()
        {
            Assert.Equal(0, _subscribeProjectionHandler.HandledMessages.Count);
        }

        [Fact]
        public void should_not_load_projection_state_handler()
        {
            Assert.Equal(0, _stateHandler._loadCalled);
        }

        [Fact]
        public void should_not_publish_started_message()
        {
            Assert.Equal(0, _consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Started>().Count());
        }
    }
}
