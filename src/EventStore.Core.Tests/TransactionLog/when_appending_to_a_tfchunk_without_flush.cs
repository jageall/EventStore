using System;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_appending_to_a_tfchunk_without_flush : IUseFixture<when_appending_to_a_tfchunk_without_flush.FixtureData>
    {
        private PrepareLogRecord _record;
        private RecordWriteResult _result;

        public class FixtureData : SpecificationWithFilePerTestFixture
        {

            private TFChunk _chunk;
            private readonly Guid _corrId = Guid.NewGuid();
            private readonly Guid _eventId = Guid.NewGuid();
            public RecordWriteResult _result;
            public PrepareLogRecord _record;

            public FixtureData()
            {
                _record = new PrepareLogRecord(0, _corrId, _eventId, 0, 0, "test", 1, new DateTime(2000, 1, 1, 12, 0, 0),
                    PrepareFlags.None, "Foo", new byte[12], new byte[15]);
                _chunk = TFChunk.CreateNew(Filename, 4096, 0, 0, false);
                _result = _chunk.TryAppend(_record);
            }

            public override void Dispose()
            {
                _chunk.Dispose();
                base.Dispose();
            }
        }

        public void SetFixture(FixtureData data)
        {
            _result = data._result;
            _record = data._record;
        }

        [Fact]
        public void the_record_is_appended()
        {
            Assert.True(_result.Success);
        }

        [Fact]
        public void the_old_position_is_returned()
        {
            //position without header.
            Assert.Equal(0, _result.OldPosition);
        }

        [Fact]
        public void the_updated_position_is_returned()
        {
            //position without header.
            Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), _result.NewPosition);
        }
    }
}