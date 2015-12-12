using System.IO;
using EventStore.Core.Tests.TransactionLog.Validation;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation
{
    public class when_truncating_into_the_middle_of_multichunk : IClassFixture<when_truncating_into_the_middle_of_multichunk.FixtureData>
    {
        private TFChunkDbConfig _config;
        private FixtureData _fixture;
        public class FixtureData :SpecificationWithDirectoryPerTestFixture
        {
            public readonly TFChunkDbConfig _config;

            public FixtureData()
            {
                _config = new TFChunkDbConfig(PathName,
                                          new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                          1000,
                                          0,
                                          new InMemoryCheckpoint(11111),
                                          new InMemoryCheckpoint(5500),
                                          new InMemoryCheckpoint(5500),
                                          new InMemoryCheckpoint(5757));

                DbUtil.CreateMultiChunk(_config, 0, 2, GetFilePathFor("chunk-000000.000001"));
                DbUtil.CreateMultiChunk(_config, 0, 2, GetFilePathFor("chunk-000000.000002"));
                DbUtil.CreateMultiChunk(_config, 3, 10, GetFilePathFor("chunk-000003.000001"));
                DbUtil.CreateMultiChunk(_config, 3, 10, GetFilePathFor("chunk-000003.000002"));
                DbUtil.CreateMultiChunk(_config, 7, 8, GetFilePathFor("chunk-000007.000001"));
                DbUtil.CreateOngoingChunk(_config, 11, GetFilePathFor("chunk-000011.000000"));

                var truncator = new TFChunkDbTruncator(_config);
                truncator.TruncateDb(_config.TruncateCheckpoint.ReadNonFlushed());
            }

            public override void Dispose()
            {
                using (var db = new TFChunkDb(_config))
                {
                    db.Open(verifyHash: false);
                }
                Assert.True(File.Exists(GetFilePathFor("chunk-000000.000002")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000003.000000")));
                Assert.Equal(2, Directory.GetFiles(PathName, "*").Length);

                base.Dispose();
            }
        }

        public void SetFixture(FixtureData data)
        {
            _config = data._config;
            _fixture = data;
        }   

        [Fact]
        public void writer_checkpoint_should_be_set_to_start_of_new_chunk()
        {
            Assert.Equal(3000, _config.WriterCheckpoint.Read());
            Assert.Equal(3000, _config.WriterCheckpoint.ReadNonFlushed());
        }

        [Fact]
        public void chaser_checkpoint_should_be_adjusted_if_less_than_actual_truncate_checkpoint()
        {
            Assert.Equal(3000, _config.ChaserCheckpoint.Read());
            Assert.Equal(3000, _config.ChaserCheckpoint.ReadNonFlushed());
        }

        [Fact]
        public void epoch_checkpoint_should_be_reset_if_less_than_actual_truncate_checkpoint()
        {
            Assert.Equal(-1, _config.EpochCheckpoint.Read());
            Assert.Equal(-1, _config.EpochCheckpoint.ReadNonFlushed());
        }

        [Fact]
        public void truncate_checkpoint_should_be_reset_after_truncation()
        {
            Assert.Equal(-1, _config.TruncateCheckpoint.Read());
            Assert.Equal(-1, _config.TruncateCheckpoint.ReadNonFlushed());
        }

        [Fact]
        public void all_excessive_chunks_should_be_deleted()
        {
            Assert.True(File.Exists(_fixture.GetFilePathFor("chunk-000000.000001")));
            Assert.True(File.Exists(_fixture.GetFilePathFor("chunk-000000.000002")));
            Assert.Equal(2, Directory.GetFiles(_fixture.PathName, "*").Length);
        }
    }
}
