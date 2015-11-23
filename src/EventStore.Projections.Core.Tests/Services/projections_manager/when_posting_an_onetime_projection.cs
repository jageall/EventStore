using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager
{
    
    public class when_posting_an_onetime_projection: TestFixtureWithProjectionCoreAndManagementServices
    {
        protected override void Given()
        {
            NoOtherStreams();
        }

        protected override IEnumerable<WhenStep> When()
        {
            yield return (new SystemMessage.BecomeMaster(Guid.NewGuid()));
            yield return
                (new ProjectionManagementMessage.Command.Post(
                    new PublishEnvelope(_bus), ProjectionManagementMessage.RunAs.Anonymous,
                    @"fromAll().whenAny(function(s,e){return s;});", enabled: true));
        }

        [Fact][Trait("Category", "v8")]
        public void projection_updated_is_published()
        {
            Assert.Equal(1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.Updated>().Count());
        }
    }
}
