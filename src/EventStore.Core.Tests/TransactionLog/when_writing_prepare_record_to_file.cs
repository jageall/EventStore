using System;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_writing_prepare_record_to_file : IClassFixture<when_writing_prepare_record_to_file.FixtureData>
    {
        private InMemoryCheckpoint _writerCheckpoint;
        private Guid _eventId;
        private Guid _correlationId;
        private PrepareLogRecord _record;
        private TFChunkDb _db;

        public class FixtureData : SpecificationWithDirectoryPerTestFixture
        {
            public InMemoryCheckpoint _writerCheckpoint;
            public readonly Guid _eventId = Guid.NewGuid();
            public readonly Guid _correlationId = Guid.NewGuid();
            public PrepareLogRecord _record;
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
                ITransactionFileWriter writer = new TFChunkWriter(_db);
                writer.Open();
                _record = new PrepareLogRecord(logPosition: 0,
                    eventId: _eventId,
                    correlationId: _correlationId,
                    transactionPosition: 0xDEAD,
                    transactionOffset: 0xBEEF,
                    eventStreamId: "WorldEnding",
                    expectedVersion: 1234,
                    timeStamp: new DateTime(2012, 12, 21),
                    flags: PrepareFlags.SingleWrite,
                    eventType: "type",
                    data: new byte[] {1, 2, 3, 4, 5},
                    metadata: new byte[] {7, 17});
                long newPos;
                writer.Write(_record, out newPos);
                writer.Flush();
            }

            public override void Dispose()
            {
                ((ITransactionFileWriter) new TFChunkWriter(_db)).Close();
                _db.Close();
                base.Dispose();
            }
        }

        public when_writing_prepare_record_to_file(FixtureData data)
        {
            _db = data._db;
            _writerCheckpoint = data._writerCheckpoint;
            _correlationId = data._correlationId;
            _eventId = data._eventId;
            _record = data._record;
        }

        [Fact]
        public void the_data_is_written()
        {
            //TODO MAKE THIS ACTUALLY ASSERT OFF THE FILE AND READER FROM KNOWN FILE
            using (var reader = new TFChunkChaser(_db, _writerCheckpoint, _db.Config.ChaserCheckpoint))
            {
                reader.Open();
                LogRecord r;
                Assert.True(reader.TryReadNext(out r));

                Assert.True(r is PrepareLogRecord);
                var p = (PrepareLogRecord) r;
                Assert.Equal(p.RecordType, LogRecordType.Prepare);
                Assert.Equal(p.LogPosition, 0);
                Assert.Equal(p.TransactionPosition, 0xDEAD);
                Assert.Equal(p.TransactionOffset, 0xBEEF);
                Assert.Equal(p.CorrelationId, _correlationId);
                Assert.Equal(p.EventId, _eventId);
                Assert.Equal(p.EventStreamId, "WorldEnding");
                Assert.Equal(p.ExpectedVersion, 1234);
                Assert.Equal(p.TimeStamp, new DateTime(2012, 12, 21));
                Assert.Equal(p.Flags, PrepareFlags.SingleWrite);
                Assert.Equal(p.EventType, "type");
                Assert.Equal(p.Data.Length, 5);
                Assert.Equal(p.Metadata.Length, 2);
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
