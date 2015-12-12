using System;
using System.Linq;
using EventStore.ClientAPI.SystemData;
using Xunit;

namespace EventStore.Projections.Core.Tests.ClientAPI
{
    public class list_projections : specification_with_standard_projections_runnning
    {
        const string TestProjection = "fromAll().when({$init: function (state, ev) {return {};},ConversationStarted: function (state, ev) {state.lastBatchSent = ev;return state;}});";

        [DebugBuildFact]
        [Trait("Category", "ClientAPI")]
        public void list_all_projections_works()
        {
            var x = _manager.ListAllAsync(new UserCredentials("admin", "changeit")).Result;
            Assert.Equal(true, x.Any());
            Assert.True(x.Any(p => p.Name == "$streams"));
        }

        [DebugBuildFact]
        [Trait("Category", "ClientAPI")]
        public void list_oneTime_projections_works()
        {
            _manager.CreateOneTimeAsync(TestProjection, new UserCredentials("admin", "changeit")).Wait();
            var x = _manager.ListOneTimeAsync(new UserCredentials("admin", "changeit")).Result;
            Assert.Equal(true, x.Any(p => p.Mode == "OneTime"));
        }

        [DebugBuildFact]
        [Trait("Category", "ClientAPI")]
        public void list_continuous_projections_works()
        {
            var nameToTest = Guid.NewGuid().ToString();
            _manager.CreateContinuousAsync(nameToTest, TestProjection, new UserCredentials("admin", "changeit")).Wait();
            var x = _manager.ListContinuousAsync(new UserCredentials("admin", "changeit")).Result;
            Assert.Equal(true, x.Any(p => p.Name == nameToTest));
        }

        public list_projections(SpecificationFixture fixture) : base(fixture)
        {
        }
    }
}