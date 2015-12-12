using System;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_writing_multiple_records_to_a_tfchunk : IClassFixture<when_writing_multiple_records_to_a_tfchunk.FixtureData>
    {
        private TFChunk _chunk;
        private long _position1;
        private long _position2;
        private bool _written1;
        private bool _written2;
        private PrepareLogRecord _prepare1;
        private PrepareLogRecord _prepare2;

        public class FixtureData : SpecificationWithFilePerTestFixture
        {
            public readonly TFChunk _chunk;
            private readonly Guid _corrId = Guid.NewGuid();
            private readonly Guid _eventId = Guid.NewGuid();
            public readonly long _position1;
            public readonly long _position2;
            public readonly bool _written1;
            public readonly bool _written2;

            public readonly PrepareLogRecord _prepare1;
            public readonly PrepareLogRecord _prepare2;

            public FixtureData()
            {
                _chunk = TFChunk.CreateNew(Filename, 4096, 0, 0, false);

                _prepare1 = new PrepareLogRecord(0, _corrId, _eventId, 0, 0, "test", 1,
                    new DateTime(2000, 1, 1, 12, 0, 0),
                    PrepareFlags.None, "Foo", new byte[12], new byte[15]);
                var r1 = _chunk.TryAppend(_prepare1);
                _written1 = r1.Success;
                _position1 = r1.OldPosition;

                _prepare2 = new PrepareLogRecord(r1.NewPosition, _corrId, _eventId, 0, 0, "test2", 2,
                    new DateTime(2000, 1, 1, 12, 0, 0),
                    PrepareFlags.None, "Foo2", new byte[12], new byte[15]);
                var r2 = _chunk.TryAppend(_prepare2);
                _written2 = r2.Success;
                _position2 = r2.OldPosition;
                _chunk.Flush();
            }

            public override void Dispose()
            {
                _chunk.Dispose();
                base.Dispose();
            }
        }

        public void SetFixture(FixtureData data)
        {
            _chunk = data._chunk;
            _position1 = data._position1;
            _position2 = data._position2;
            _written1 = data._written1;
            _written2 = data._written2;
            _prepare1 = data._prepare1;
            _prepare2 = data._prepare2;
        }

        [Fact]
        public void the_chunk_is_not_cached()
        {
            Assert.False(_chunk.IsCached);
        }

        [Fact]
        public void the_first_record_was_written()
        {
            Assert.True(_written1);
        }

        [Fact]
        public void the_second_record_was_written()
        {
            Assert.True(_written2);
        }

        [Fact]
        public void the_first_record_can_be_read_at_position()
        {
            var res = _chunk.TryReadAt((int)_position1);
            Assert.True(res.Success);
            Assert.True(res.LogRecord is PrepareLogRecord);
            Assert.Equal(_prepare1, res.LogRecord);
        }

        [Fact]
        public void the_second_record_can_be_read_at_position()
        {
            var res = _chunk.TryReadAt((int)_position2);
            Assert.True(res.Success);
            Assert.True(res.LogRecord is PrepareLogRecord);
            Assert.Equal(_prepare2, res.LogRecord);
        }

        [Fact] 
        public void the_first_record_can_be_read()
        {
            var res = _chunk.TryReadFirst();
            Assert.True(res.Success);
            Assert.Equal(_prepare1.GetSizeWithLengthPrefixAndSuffix(), res.NextPosition);
            Assert.True(res.LogRecord is PrepareLogRecord);
            Assert.Equal(_prepare1, res.LogRecord);
        }

        [Fact]
        public void the_second_record_can_be_read_as_closest_forward_after_first()
        {
            var res = _chunk.TryReadClosestForward(_prepare1.GetSizeWithLengthPrefixAndSuffix());
            Assert.True(res.Success);
            Assert.Equal(_prepare1.GetSizeWithLengthPrefixAndSuffix()
                            + _prepare2.GetSizeWithLengthPrefixAndSuffix(), res.NextPosition);
            Assert.True(res.LogRecord is PrepareLogRecord);
            Assert.Equal(_prepare2, res.LogRecord);
        }

        [Fact]
        public void cannot_read_past_second_record_with_closest_forward_method()
        {
            var res = _chunk.TryReadClosestForward(_prepare1.GetSizeWithLengthPrefixAndSuffix() 
                                                  + _prepare2.GetSizeWithLengthPrefixAndSuffix());
            Assert.False(res.Success);
        }

        [Fact]
        public void the_seconds_record_can_be_read_as_last()
        {
            var res = _chunk.TryReadLast();
            Assert.True(res.Success);
            Assert.Equal(_prepare1.GetSizeWithLengthPrefixAndSuffix(), res.NextPosition);
            Assert.Equal(_prepare2, res.LogRecord);
        }

        [Fact]
        public void the_first_record_can_be_read_as_closest_backward_after_last()
        {
            var res = _chunk.TryReadClosestBackward(_prepare1.GetSizeWithLengthPrefixAndSuffix());
            Assert.True(res.Success);
            Assert.Equal(0, res.NextPosition);
            Assert.Equal(_prepare1, res.LogRecord);
        }

        [Fact]
        public void cannot_read_backward_from_zero_pos()
        {
            var res = _chunk.TryReadClosestBackward(0);
            Assert.False(res.Success);
        }
    }
}