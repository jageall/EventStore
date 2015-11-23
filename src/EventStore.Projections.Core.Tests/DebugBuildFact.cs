using Xunit;

namespace EventStore.Projections.Core.Tests
{
    public sealed class DebugBuildFact : FactAttribute
    {
        public DebugBuildFact()
        {
#if !DEBUG
            Skip = "Requires debug build";
#endif
        }
    }
}
