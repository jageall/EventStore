using System.IO;
using EventStore.Core.Exceptions;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Validation
{
    public class when_validating_tfchunk_db_with_multi_chunks : SpecificationWithDirectory
    {
        [Fact]
        public void with_not_enough_files_to_reach_checksum_throws()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             10000,
                                             0,
                                             new InMemoryCheckpoint(25000),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateMultiChunk(config, 0, 1, GetFilePathFor("chunk-000000.000000"));
                var thrown = Assert.Throws<CorruptDatabaseException>(() => db.Open(verifyHash: false));
                Assert.IsType<ChunkNotFoundException>(thrown.InnerException);
            }
        }

        [Fact]
        public void with_checksum_inside_multi_chunk_throws()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             10000,
                                             0,
                                             new InMemoryCheckpoint(25000),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateMultiChunk(config, 0, 2, GetFilePathFor("chunk-000000.000000"));
                var thrown = Assert.Throws<CorruptDatabaseException>(() => db.Open(verifyHash: false));
                Assert.IsType<ChunkNotFoundException>(thrown.InnerException);
            }
        }

        [Fact]
        public void allows_with_exactly_enough_file_to_reach_checksum_while_last_is_multi_chunk()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             10000,
                                             0,
                                             new InMemoryCheckpoint(30000),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateSingleChunk(config, 0, GetFilePathFor("chunk-000000.000000"));
                DbUtil.CreateMultiChunk(config, 1, 2, GetFilePathFor("chunk-000001.000000"));
                Assert.DoesNotThrow(() => db.Open(verifyHash: false));

                Assert.True(File.Exists(GetFilePathFor("chunk-000000.000000")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000001.000000")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000003.000000")));
                Assert.Equal(3, Directory.GetFiles(PathName, "*").Length);
            }
        }

        [Fact]
        public void allows_next_new_chunk_when_checksum_is_exactly_in_between_two_chunks_if_last_is_ongoing_chunk()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             10000,
                                             0,
                                             new InMemoryCheckpoint(20000),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateMultiChunk(config, 0, 1, GetFilePathFor("chunk-000000.000000"));
                DbUtil.CreateOngoingChunk(config, 2, GetFilePathFor("chunk-000002.000000"));
                Assert.DoesNotThrow(() => db.Open(verifyHash: false));

                Assert.True(File.Exists(GetFilePathFor("chunk-000000.000000")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000002.000000")));
                Assert.Equal(2, Directory.GetFiles(PathName, "*").Length);
            }
        }

        [Fact(Skip="Due to truncation such situation can happen, so must be considered valid.")]
        public void does_not_allow_next_new_chunk_when_checksum_is_exactly_in_between_two_chunks_and_last_is_multi_chunk()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             10000,
                                             0,
                                             new InMemoryCheckpoint(10000),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateSingleChunk(config, 0, GetFilePathFor("chunk-000000.000000"));
                DbUtil.CreateMultiChunk(config, 1, 2, GetFilePathFor("chunk-000001.000000"));
                var thrown = Assert.Throws<CorruptDatabaseException>(() => db.Open(verifyHash: false));
                Assert.IsType<BadChunkInDatabaseException>(thrown.InnerException);
            }
        }

        [Fact]
        public void old_version_of_chunks_are_removed()
        {
            File.Create(GetFilePathFor("foo")).Close();
            File.Create(GetFilePathFor("bla")).Close();

            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             100,
                                             0,
                                             new InMemoryCheckpoint(350),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateSingleChunk(config, 0, GetFilePathFor("chunk-000000.000000"));
                DbUtil.CreateSingleChunk(config, 0, GetFilePathFor("chunk-000000.000002"));
                DbUtil.CreateSingleChunk(config, 0, GetFilePathFor("chunk-000000.000005"));
                DbUtil.CreateSingleChunk(config, 1, GetFilePathFor("chunk-000001.000000"));
                DbUtil.CreateMultiChunk(config, 1, 2, GetFilePathFor("chunk-000001.000001"));
                DbUtil.CreateSingleChunk(config, 2, GetFilePathFor("chunk-000002.000000"));
                DbUtil.CreateSingleChunk(config, 3, GetFilePathFor("chunk-000003.000007"));
                DbUtil.CreateOngoingChunk(config, 3, GetFilePathFor("chunk-000003.000008"));

                Assert.DoesNotThrow(() => db.Open(verifyHash: false));

                Assert.True(File.Exists(GetFilePathFor("foo")));
                Assert.True(File.Exists(GetFilePathFor("bla")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000000.000005")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000001.000001")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000003.000008")));
                Assert.Equal(5, Directory.GetFiles(PathName, "*").Length);
            }
        }

        [Fact]
        public void when_checkpoint_is_exactly_on_the_boundary_of_chunk_the_last_chunk_could_be_not_present_but_should_be_created()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             100,
                                             0,
                                             new InMemoryCheckpoint(200),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateMultiChunk(config, 0, 1, GetFilePathFor("chunk-000000.000000"));

                Assert.DoesNotThrow(() => db.Open(verifyHash: false));
                Assert.NotNull(db.Manager.GetChunk(2));

                Assert.True(File.Exists(GetFilePathFor("chunk-000000.000000")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000002.000000")));
                Assert.Equal(2, Directory.GetFiles(PathName, "*").Length);
            }
        }

        [Fact]
        public void when_checkpoint_is_exactly_on_the_boundary_of_chunk_the_last_chunk_could_be_present()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             100,
                                             0,
                                             new InMemoryCheckpoint(200),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateMultiChunk(config, 0, 1, GetFilePathFor("chunk-000000.000000"));
                DbUtil.CreateOngoingChunk(config, 2, GetFilePathFor("chunk-000002.000001"));

                Assert.DoesNotThrow(() => db.Open(verifyHash: false));
                Assert.NotNull(db.Manager.GetChunk(2));

                Assert.True(File.Exists(GetFilePathFor("chunk-000000.000000")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000002.000001")));
                Assert.Equal(2, Directory.GetFiles(PathName, "*").Length);
            }
        }

        [Fact]
        public void when_checkpoint_is_on_boundary_of_new_chunk_and_last_chunk_is_truncated_no_exception_is_thrown()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             100,
                                             0,
                                             new InMemoryCheckpoint(300),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateSingleChunk(config, 0, GetFilePathFor("chunk-000000.000000"));
                DbUtil.CreateMultiChunk(config, 1, 2, GetFilePathFor("chunk-000001.000001"), physicalSize: 50, logicalSize: 150);

                Assert.DoesNotThrow(() => db.Open(verifyHash: false));
                Assert.NotNull(db.Manager.GetChunk(2));

                Assert.True(File.Exists(GetFilePathFor("chunk-000000.000000")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000001.000001")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000003.000000")));
                Assert.Equal(3, Directory.GetFiles(PathName, "*").Length);
            }
        }

        [Fact]
        public void does_not_allow_checkpoint_to_point_into_the_middle_of_multichunk_chunk()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             1000,
                                             0,
                                             new InMemoryCheckpoint(1500),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateSingleChunk(config, 0, GetFilePathFor("chunk-000000.000000"));
                DbUtil.CreateMultiChunk(config, 1, 10, GetFilePathFor("chunk-000001.000001"));

                var thrown = Assert.Throws<CorruptDatabaseException>(() => db.Open(verifyHash: false));
                Assert.IsType<BadChunkInDatabaseException>(thrown.InnerException);
            }
        }

        [Fact]
        public void allows_last_chunk_to_be_multichunk_when_checkpoint_point_at_the_start_of_next_chunk()
        {
            var config = new TFChunkDbConfig(PathName,
                                             new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                             1000,
                                             0,
                                             new InMemoryCheckpoint(4000),
                                             new InMemoryCheckpoint(),
                                             new InMemoryCheckpoint(-1),
                                             new InMemoryCheckpoint(-1));
            using (var db = new TFChunkDb(config))
            {
                DbUtil.CreateSingleChunk(config, 0, GetFilePathFor("chunk-000000.000000"));
                DbUtil.CreateMultiChunk(config, 1, 3, GetFilePathFor("chunk-000001.000001"));

                Assert.DoesNotThrow(() => db.Open(verifyHash: false));

                Assert.True(File.Exists(GetFilePathFor("chunk-000000.000000")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000001.000001")));
                Assert.True(File.Exists(GetFilePathFor("chunk-000004.000000")));
                Assert.Equal(3, Directory.GetFiles(PathName, "*").Length);
            }
        }
    }
}