using System;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_uncaching_a_tfchunk : IClassFixture<when_uncaching_a_tfchunk.FixtureData>
    {
        private RecordWriteResult _result;
        private PrepareLogRecord _record;
        private TFChunk _uncachedChunk;

        public class FixtureData : SpecificationWithFilePerTestFixture
        {
            private readonly TFChunk _chunk;
            private readonly Guid _corrId = Guid.NewGuid();
            private readonly Guid _eventId = Guid.NewGuid();
            public RecordWriteResult _result;
            public readonly PrepareLogRecord _record;
            public readonly TFChunk _uncachedChunk;

            public FixtureData()
            {
                _record = new PrepareLogRecord(0, _corrId, _eventId, 0, 0, "test", 1, new DateTime(2000, 1, 1, 12, 0, 0),
                    PrepareFlags.None, "Foo", new byte[12], new byte[15]);
                _chunk = TFChunk.CreateNew(Filename, 4096, 0, 0, false);
                _result = _chunk.TryAppend(_record);
                _chunk.Flush();
                _chunk.Complete();
                _uncachedChunk = TFChunk.FromCompletedFile(Filename, verifyHash: true);
                _uncachedChunk.CacheInMemory();
                _uncachedChunk.UnCacheFromMemory();
            }

            public override void Dispose()
            {
                _chunk.Dispose();
                _uncachedChunk.Dispose();
                base.Dispose();
            }
        }

        public when_uncaching_a_tfchunk(FixtureData data)
        {
            _result = data._result;
            _record = data._record;
            _uncachedChunk = data._uncachedChunk;
        }

        [Fact]
        public void the_write_result_is_correct()
        {
            Assert.True(_result.Success);
            Assert.Equal(0, _result.OldPosition);
            Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), _result.NewPosition);
        }

        [Fact]
        public void the_chunk_is_not_cached()
        {
            Assert.False(_uncachedChunk.IsCached);
        }

        [Fact]
        public void the_record_was_written()
        {
            Assert.True(_result.Success);
        }

        [Fact]
        public void the_correct_position_is_returned()
        {
            Assert.Equal(0, _result.OldPosition);
        }

        [Fact]
        public void the_record_can_be_read()
        {
            var res = _uncachedChunk.TryReadAt(0);
            Assert.True(res.Success);
            Assert.Equal(_record, res.LogRecord);
            Assert.Equal(_result.OldPosition, res.LogRecord.LogPosition);
            //Assert.Equal(_result.NewPosition, res.NewPosition);
        }
    }
}