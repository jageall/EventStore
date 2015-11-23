using System;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_reading_from_a_cached_tfchunk : IUseFixture<when_reading_from_a_cached_tfchunk.FixtureData>
    {
        private RecordWriteResult _result;
        private PrepareLogRecord _record;
        private TFChunk _cachedChunk;

        public class FixtureData : SpecificationWithFilePerTestFixture
        {
            private TFChunk _chunk;
            private readonly Guid _corrId = Guid.NewGuid();
            private readonly Guid _eventId = Guid.NewGuid();
            public TFChunk _cachedChunk;
            public PrepareLogRecord _record;
            public RecordWriteResult _result;

            public FixtureData()
            {
                _record = new PrepareLogRecord(0, _corrId, _eventId, 0, 0, "test", 1, new DateTime(2000, 1, 1, 12, 0, 0),
                    PrepareFlags.None, "Foo", new byte[12], new byte[15]);
                _chunk = TFChunk.CreateNew(Filename, 4096, 0, 0, false);
                _result = _chunk.TryAppend(_record);
                _chunk.Flush();
                _chunk.Complete();
                _cachedChunk = TFChunk.FromCompletedFile(Filename, verifyHash: true);
                _cachedChunk.CacheInMemory();
            }

            public override void Dispose()
            {
                _chunk.Dispose();
                _cachedChunk.Dispose();
                base.Dispose();
            }
        }

        public void SetFixture(FixtureData data)
        {
            _result = data._result;
            _record = data._record;
            _cachedChunk = data._cachedChunk;
        }

        [Fact]
        public void the_write_result_is_correct()
        {
            Assert.True(_result.Success);
            Assert.Equal(0, _result.OldPosition);
            Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), _result.NewPosition);
        }

        [Fact]
        public void the_chunk_is_cached()
        {
            Assert.True(_cachedChunk.IsCached);
        }

        [Fact]
        public void the_record_can_be_read_at_exact_position()
        {
            var res = _cachedChunk.TryReadAt(0);
            Assert.True(res.Success);
            Assert.Equal(_record, res.LogRecord);
            Assert.Equal(_result.OldPosition, res.LogRecord.LogPosition);
        }

        [Fact]
        public void the_record_can_be_read_as_first_record()
        {
            var res = _cachedChunk.TryReadFirst();
            Assert.True(res.Success);
            Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), res.NextPosition);
            Assert.Equal(_record, res.LogRecord);
            Assert.Equal(_result.OldPosition, res.LogRecord.LogPosition);
        }

        [Fact]
        public void the_record_can_be_read_as_closest_forward_to_zero_pos()
        {
            var res = _cachedChunk.TryReadClosestForward(0);
            Assert.True(res.Success);
            Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), res.NextPosition);
            Assert.Equal(_record, res.LogRecord);
            Assert.Equal(_result.OldPosition, res.LogRecord.LogPosition);
        }

        [Fact]
        public void the_record_can_be_read_as_closest_backward_from_end()
        {
            var res = _cachedChunk.TryReadClosestBackward(_record.GetSizeWithLengthPrefixAndSuffix());
            Assert.True(res.Success);
            Assert.Equal(0, res.NextPosition);
            Assert.Equal(_record, res.LogRecord);
        }

        [Fact]
        public void the_record_can_be_read_as_last()
        {
            var res = _cachedChunk.TryReadLast();
            Assert.True(res.Success);
            Assert.Equal(0, res.NextPosition);
            Assert.Equal(_record, res.LogRecord);
        }
    }
}