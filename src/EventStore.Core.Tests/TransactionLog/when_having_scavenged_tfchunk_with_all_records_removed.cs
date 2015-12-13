using System;
using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.Index.Hashes;
using EventStore.Core.Tests.Services.Storage;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_having_scavenged_tfchunk_with_all_records_removed: IClassFixture<when_having_scavenged_tfchunk_with_all_records_removed.Fixture>
    {
        public class Fixture : SpecificationWithDirectoryPerTestFixture
        {
            private TFChunkDb _db;
            public TFChunk _scavengedChunk;
            public PrepareLogRecord _p1, _p2, _p3;
            public CommitLogRecord _c1, _c2, _c3;
            public RecordWriteResult _res1, _res2, _res3;
            public RecordWriteResult _cres1, _cres2, _cres3;

            public Fixture()
            {
                _db = new TFChunkDb(new TFChunkDbConfig(PathName,
                    new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                    16*1024,
                    0,
                    new InMemoryCheckpoint(),
                    new InMemoryCheckpoint(),
                    new InMemoryCheckpoint(-1),
                    new InMemoryCheckpoint(-1)));
                _db.Open();

                var chunk = _db.Manager.GetChunkFor(0);

                _p1 = LogRecord.SingleWrite(0, Guid.NewGuid(), Guid.NewGuid(), "es-to-scavenge", ExpectedVersion.Any,
                    "et1",
                    new byte[] {0, 1, 2}, new byte[] {5, 7});
                _res1 = chunk.TryAppend(_p1);

                _c1 = LogRecord.Commit(_res1.NewPosition, Guid.NewGuid(), _p1.LogPosition, 0);
                _cres1 = chunk.TryAppend(_c1);

                _p2 = LogRecord.SingleWrite(_cres1.NewPosition,
                    Guid.NewGuid(), Guid.NewGuid(), "es-to-scavenge", ExpectedVersion.Any, "et1",
                    new byte[] {0, 1, 2}, new byte[] {5, 7});
                _res2 = chunk.TryAppend(_p2);

                _c2 = LogRecord.Commit(_res2.NewPosition, Guid.NewGuid(), _p2.LogPosition, 1);
                _cres2 = chunk.TryAppend(_c2);

                _p3 = LogRecord.SingleWrite(_cres2.NewPosition,
                    Guid.NewGuid(), Guid.NewGuid(), "es-to-scavenge", ExpectedVersion.Any, "et1",
                    new byte[] {0, 1, 2}, new byte[] {5, 7});
                _res3 = chunk.TryAppend(_p3);

                _c3 = LogRecord.Commit(_res3.NewPosition, Guid.NewGuid(), _p3.LogPosition, 2);
                _cres3 = chunk.TryAppend(_c3);

                chunk.Complete();

                _db.Config.WriterCheckpoint.Write(chunk.ChunkHeader.ChunkEndPosition);
                _db.Config.WriterCheckpoint.Flush();
                _db.Config.ChaserCheckpoint.Write(chunk.ChunkHeader.ChunkEndPosition);
                _db.Config.ChaserCheckpoint.Flush();

                var scavenger = new TFChunkScavenger(_db, new FakeTableIndex(), new XXHashUnsafe(),
                    new FakeReadIndex(x => x == "es-to-scavenge"));
                scavenger.Scavenge(alwaysKeepScavenged: true, mergeChunks: false);

                _scavengedChunk = _db.Manager.GetChunk(0);
            }

            public override void Dispose()
            {
                _db.Dispose();

                base.Dispose();
            }
        }
        private TFChunk _scavengedChunk;
        private PrepareLogRecord _p1, _p2, _p3;
        private CommitLogRecord _c1, _c2, _c3;
        private RecordWriteResult _res1, _res2, _res3;
        private RecordWriteResult _cres1, _cres2, _cres3;

        public when_having_scavenged_tfchunk_with_all_records_removed(Fixture fixture)
        {
            _scavengedChunk = fixture._scavengedChunk;
            _p1 = fixture._p1;
            _p2 = fixture._p2;
            _p3 = fixture._p3;
            _c1 = fixture._c1;
            _c2 = fixture._c2;
            _c3 = fixture._c3;
            _res1 = fixture._res1;
            _res2 = fixture._res2;
            _res3 = fixture._res3;
            _cres1 = fixture._cres1;
            _cres2 = fixture._cres2;
            _cres3 = fixture._cres3;
        }

        [Fact]
        public void first_record_was_written()
        {
            Assert.True(_res1.Success);
            Assert.True(_cres1.Success);
        }

        [Fact]
        public void second_record_was_written()
        {
            Assert.True(_res2.Success);
            Assert.True(_cres2.Success);
        }

        [Fact]
        public void third_record_was_written()
        {
            Assert.True(_res3.Success);
            Assert.True(_cres3.Success);
        }

        [Fact]
        public void prepare1_cant_be_read_at_position()
        {
            var res = _scavengedChunk.TryReadAt((int)_p1.LogPosition);
            Assert.False(res.Success);
        }

        [Fact]
        public void commit1_cant_be_read_at_position()
        {
            var res = _scavengedChunk.TryReadAt((int)_c1.LogPosition);
            Assert.False(res.Success);
        }

        [Fact]
        public void prepare2_cant_be_read_at_position()
        {
            var res = _scavengedChunk.TryReadAt((int)_p2.LogPosition);
            Assert.False(res.Success);
        }

        [Fact]
        public void commit2_cant_be_read_at_position()
        {
            var res = _scavengedChunk.TryReadAt((int)_c2.LogPosition);
            Assert.False(res.Success);
        }

        [Fact]
        public void prepare3_cant_be_read_at_position()
        {
            var res = _scavengedChunk.TryReadAt((int)_p3.LogPosition);
            Assert.False(res.Success);
        }

        [Fact]
        public void commit3_cant_be_read_at_position()
        {
            var res = _scavengedChunk.TryReadAt((int)_c3.LogPosition);
            Assert.False(res.Success);
        }

        [Fact]
        public void sequencial_read_returns_no_records()
        {
            var records = new List<LogRecord>();
            RecordReadResult res = _scavengedChunk.TryReadFirst();
            while (res.Success)
            {
                records.Add(res.LogRecord);
                res = _scavengedChunk.TryReadClosestForward((int)res.NextPosition);
            }
            Assert.Equal(0, records.Count);
        }
    }
}