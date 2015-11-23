using Xunit;

namespace EventStore.Projections.Core.Tests
{
    public class TestsInitFixture
    {
        private readonly EventStore.Core.Tests.TestsInitFixture _initFixture = new EventStore.Core.Tests.TestsInitFixture();

        public void SetUp()
        {
            _initFixture.SetUp();
        }

        public void TearDown()
        {
            _initFixture.TearDown();
        }
    }
}
