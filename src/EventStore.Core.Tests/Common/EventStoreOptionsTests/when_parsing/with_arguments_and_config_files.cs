using EventStore.Common.Options;
using EventStore.Core.Util;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.Common.EventStoreOptionsTests.when_parsing
{
    public class with_arguments_and_config_files
    {
        [Fact]
        public void should_use_the_argument_over_the_config_file_value()
        {
            var args = new string[] { "-config", "TestConfigs/test_config.yaml", "-log", "~/customLogsDirectory" };
            var testArgs = EventStoreOptions.Parse<TestArgs>(args, Opts.EnvPrefix);
            Assert.Equal("~/customLogsDirectory", testArgs.Log);
        }
    }
}
