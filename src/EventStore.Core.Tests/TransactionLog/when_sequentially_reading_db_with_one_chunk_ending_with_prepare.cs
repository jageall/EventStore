using System;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_sequentially_reading_db_with_one_chunk_ending_with_prepare : IClassFixture<when_sequentially_reading_db_with_one_chunk_ending_with_prepare.Fixture>
    {
        private TFChunkDb _db;
        private const int RecordsCount = 3;

        public class Fixture : SpecificationWithDirectoryPerTestFixture
        {
            public readonly TFChunkDb _db;
            private LogRecord[] _records;
            private RecordWriteResult[] _results;

            public Fixture()
            {
                _db =
                    new TFChunkDb(
                        new TFChunkDbConfig(
                            PathName,
                            new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                            4096,
                            0,
                            new InMemoryCheckpoint(),
                            new InMemoryCheckpoint(),
                            new InMemoryCheckpoint(-1),
                            new InMemoryCheckpoint(-1)));
                _db.Open();

                var chunk = _db.Manager.GetChunk(0);

                _records = new LogRecord[RecordsCount];
                _results = new RecordWriteResult[RecordsCount];

                for (int i = 0; i < _records.Length - 1; ++i)
                {
                    _records[i] = LogRecord.SingleWrite(
                        i == 0 ? 0 : _results[i - 1].NewPosition,
                        Guid.NewGuid(),
                        Guid.NewGuid(),
                        "es1",
                        ExpectedVersion.Any,
                        "et1",
                        new byte[] {0, 1, 2},
                        new byte[] {5, 7});
                    _results[i] = chunk.TryAppend(_records[i]);
                }
                _records[_records.Length - 1] = LogRecord.Prepare(
                    _results[_records.Length - 1 - 1].NewPosition,
                    Guid.NewGuid(),
                    Guid.NewGuid(),
                    _results[_records.Length - 1 - 1].NewPosition,
                    0,
                    "es1",
                    ExpectedVersion.Any,
                    PrepareFlags.Data,
                    "et1",
                    new byte[] {0, 1, 2},
                    new byte[] {5, 7});
                _results[_records.Length - 1] = chunk.TryAppend(_records[_records.Length - 1]);

                chunk.Flush();
                _db.Config.WriterCheckpoint.Write(_results[RecordsCount - 1].NewPosition);
                _db.Config.WriterCheckpoint.Flush();
            }

            public override void Dispose()
            {
                _db.Dispose();

                base.Dispose();
            }
        }

        public void SetFixture(Fixture fixture)
        {
            _db = fixture._db;
        }

        [Fact]
        public void only_the_last_record_is_marked_eof()
        {
            var seqReader = new TFChunkReader(_db, _db.Config.WriterCheckpoint, 0);

            SeqReadResult res;
            int count = 0;
            while ((res = seqReader.TryReadNext()).Success)
            {
                ++count;
                Assert.Equal(count == RecordsCount, res.Eof);
            }
            Assert.Equal(RecordsCount, count);
        }

    }
}
