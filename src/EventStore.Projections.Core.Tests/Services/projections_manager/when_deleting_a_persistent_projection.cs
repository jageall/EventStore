using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager
{
    
    public class when_deleting_a_persistent_projection : TestFixtureWithProjectionCoreAndManagementServices
    {
        private string _projectionName;
        private const string _projectionCheckpointStream = "$projections-test-projection-checkpoint";

        protected override void Given()
        {
            _projectionName = "test-projection";
            AllWritesSucceed();
            NoOtherStreams();
        }

        protected override IEnumerable<WhenStep> When()
        {
            yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
            yield return
                new ProjectionManagementMessage.Command.Post(
                    new PublishEnvelope(_bus), ProjectionMode.Continuous, _projectionName,
                    ProjectionManagementMessage.RunAs.System, "JS", @"fromAll().whenAny(function(s,e){return s;});",
                    enabled: true, checkpointsEnabled: true, emitEnabled: true);
            yield return
                new ProjectionManagementMessage.Command.Disable(
                    new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System);
            yield return
                new ProjectionManagementMessage.Command.Delete(
                    new PublishEnvelope(_bus), _projectionName,
                    ProjectionManagementMessage.RunAs.System, true, true);
        }

        [Fact][Trait("Category", "v8")]
        public void a_projection_deleted_event_is_written()
        {
            Assert.Equal(
                "$ProjectionDeleted",
                _consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Last().Events[0].EventType);
            Assert.Equal(
                _projectionName,
                Helper.UTF8NoBom.GetString(_consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Last().Events[0].Data));
        }

        [Fact][Trait("Category", "v8")]
        public void should_have_attempted_to_delete_the_checkpoint_stream()
        {
            Assert.True(
                _consumer.HandledMessages.OfType<ClientMessage.DeleteStream>().Any(x=>x.EventStreamId == _projectionCheckpointStream));
        }
    }
}
