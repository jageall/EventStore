using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager
{
    
    public class when_a_disabled_projection_has_been_loaded : TestFixtureWithProjectionCoreAndManagementServices
    {
        protected override void Given()
        {
            base.Given();
            NoStream("$projections-test-projection-result");
            NoStream("$projections-test-projection-order");
            AllWritesToSucceed("$projections-test-projection-order");
            NoStream("$projections-test-projection-checkpoint");
            ExistingEvent("$projections-$all", "$ProjectionCreated", null, "test-projection");
            ExistingEvent(
                "$projections-test-projection", "$ProjectionUpdated", null,
                @"{
                    ""Query"":""fromAll(); on_any(function(){});log('hello-from-projection-definition');"", 
                    ""Mode"":""3"", 
                    ""Enabled"":false, 
                    ""HandlerType"":""JS"",
                    ""SourceDefinition"":{
                        ""AllEvents"":true,
                        ""AllStreams"":true,
                    }
                }");
            AllWritesSucceed();
        }

        private string _projectionName;

        protected override IEnumerable<WhenStep> When()
        {
            _projectionName = "test-projection";
            yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
        }

        [Fact]
        public void the_projection_source_can_be_retrieved()
        {
            _manager.Handle(
                new ProjectionManagementMessage.Command.GetQuery(
                    new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.Anonymous));
            Assert.Equal(1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Count());
            var projectionQuery =
                _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Single();
            Assert.Equal(_projectionName, projectionQuery.Name);
        }

        [Fact]
        public void the_projection_status_is_stopped()
        {
            _manager.Handle(
                new ProjectionManagementMessage.Command.GetStatistics(new PublishEnvelope(_bus), null, _projectionName, false));

            Assert.Equal(1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Count());
            Assert.Equal(
                1,
                _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections.Length);
            Assert.Equal(
                _projectionName,
                _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections.Single()
                    .Name);
            Assert.Equal(
                ManagedProjectionState.Stopped,
                _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections.Single()
                    .MasterStatus);
        }

        [Fact]
        public void the_projection_state_can_be_retrieved()
        {
            _manager.Handle(new ProjectionManagementMessage.Command.GetState(new PublishEnvelope(_bus), _projectionName, ""));
            _queue.Process();

            Assert.Equal(1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Count());
            Assert.Equal(
                _projectionName,
                _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Single().Name);
            Assert.Equal(
                "", _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Single().State);
        }
    }
}
