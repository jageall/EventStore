using System.Collections.Generic;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.projection_manager_response_reader
{
    
    public class when_starting : specification_with_projection_manager_response_reader
    {
        protected override IEnumerable<WhenStep> When()
        {
            yield return new ProjectionManagementMessage.Starting();
        }

        [Fact]
        public void registers_core_service()
        {
            //This is probably doing something useful... somewhere... somehow
        }
    }
}
