using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_system
{
    namespace startup
    {
        
        public class when_starting_with_empty_db : with_projections_subsystem
        {
            protected override IEnumerable<WhenStep> When()
            {
                yield return
                    new ProjectionManagementMessage.Command.GetStatistics(Envelope, ProjectionMode.AllNonTransient, null, false)
                    ;
            }

            [Fact]
            public void system_projections_are_registered()
            {
                var statistics = HandledMessages.OfType<ProjectionManagementMessage.Statistics>().LastOrDefault();
                Assert.NotNull(statistics);
                Assert.Equal(4, statistics.Projections.Length);
            }

            [Fact]
            public void system_projections_are_running()
            {
                var statistics = HandledMessages.OfType<ProjectionManagementMessage.Statistics>().LastOrDefault();
                Assert.NotNull(statistics);
                Assert.True(statistics.Projections.All(s => s.Status == "Stopped"));
            }

        }
    }
}
