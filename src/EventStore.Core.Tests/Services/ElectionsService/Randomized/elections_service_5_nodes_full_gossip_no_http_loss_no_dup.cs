using System;
using System.Linq;
using Xunit;
using Xunit.Extensions;

namespace EventStore.Core.Tests.Services.ElectionsService.Randomized
{
    public class elections_service_5_nodes_full_gossip_no_http_loss_no_dup
    {
        private RandomizedElectionsTestCase _randomCase;

        public elections_service_5_nodes_full_gossip_no_http_loss_no_dup()
        {
            _randomCase = new RandomizedElectionsTestCase(ElectionParams.MaxIterationCount,
                                                          instancesCnt: 5,
                                                          httpLossProbability: 0.0,
                                                          httpDupProbability: 0.0,
                                                          httpMaxDelay: 20,
                                                          timerMinDelay: 100,
                                                          timerMaxDelay: 200);
            _randomCase.Init();
        }

        [Theory]
        [Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        [Trait("Category", "Explicit")]
        [PropertyData("TestRuns", PropertyType = typeof(ElectionParams))]
        public void should_always_arrive_at_coherent_results(int run)
        {
            var success = _randomCase.Run();
            if (!success)
                _randomCase.Logger.LogMessages();
            Console.WriteLine("There were a total of {0} messages in this run.", _randomCase.Logger.ProcessedItems.Count());
            Assert.True(success);
        }

        [Theory]
        [Trait("Category", "LongRunning")]
        [Trait("Category", "Network")]
        [PropertyData("TenRuns", PropertyType = typeof(ElectionParams))]
        public void should_always_arrive_at_coherent_results2(int run)
        {
            var success = _randomCase.Run();
            if (!success)
                _randomCase.Logger.LogMessages();
            Console.WriteLine("There were a total of {0} messages in this run.", _randomCase.Logger.ProcessedItems.Count());
            Assert.True(success);
        }
    }
}