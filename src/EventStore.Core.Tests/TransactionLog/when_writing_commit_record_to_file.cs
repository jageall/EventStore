using System;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_writing_commit_record_to_file: IUseFixture<when_writing_commit_record_to_file.FixtureData> 
    {
        private TFChunkDb _db;
        private InMemoryCheckpoint _writerCheckpoint;
        private CommitLogRecord _record;
        private Guid _eventId;

        public class FixtureData : SpecificationWithDirectoryPerTestFixture
        {
            private ITransactionFileWriter _writer;
            public InMemoryCheckpoint _writerCheckpoint;
            public readonly Guid _eventId = Guid.NewGuid();
            public CommitLogRecord _record;
            public TFChunkDb _db;

            public FixtureData()
            {
                _writerCheckpoint = new InMemoryCheckpoint();
                _db = new TFChunkDb(new TFChunkDbConfig(PathName,
                    new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                    1024,
                    0,
                    _writerCheckpoint,
                    new InMemoryCheckpoint(),
                    new InMemoryCheckpoint(-1),
                    new InMemoryCheckpoint(-1)));
                _db.Open();
                _writer = new TFChunkWriter(_db);
                _writer.Open();
                _record = new CommitLogRecord(logPosition: 0,
                    correlationId: _eventId,
                    transactionPosition: 4321,
                    timeStamp: new DateTime(2012, 12, 21),
                    firstEventNumber: 10);
                long newPos;
                _writer.Write(_record, out newPos);
                _writer.Flush();
            }

            public override void Dispose()
            {
                _writer.Close();
                _db.Close();
                base.Dispose();
            }
        }

        public void SetFixture(FixtureData data)
        {
            _db = data._db;
            _writerCheckpoint = data._writerCheckpoint;
            _record = data._record;
            _eventId = data._eventId;
        }

        [Fact]
        public void the_data_is_written()
        {
            using (var reader = new TFChunkChaser(_db, _writerCheckpoint, _db.Config.ChaserCheckpoint))
            {
                reader.Open();
                LogRecord r;
                Assert.True(reader.TryReadNext(out r));

                Assert.True(r is CommitLogRecord);
                var c = (CommitLogRecord) r;
                Assert.Equal(c.RecordType, LogRecordType.Commit);
                Assert.Equal(c.LogPosition, 0);
                Assert.Equal(c.CorrelationId, _eventId);
                Assert.Equal(c.TransactionPosition, 4321);
                Assert.Equal(c.TimeStamp, new DateTime(2012, 12, 21));
            }
        }

        [Fact]
        public void the_checksum_is_updated()
        {
            Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), _writerCheckpoint.Read());
        }

        [Fact]
        public void trying_to_read_past_writer_checksum_returns_false()
        {
            var reader = new TFChunkReader(_db, _writerCheckpoint);
            Assert.False(reader.TryReadAt(_writerCheckpoint.Read()).Success);
        }
    }
}