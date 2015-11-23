using EventStore.Core.TransactionLog.Chunks.TFChunk;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_reading_cached_empty_scavenged_tfchunk : IUseFixture<when_reading_cached_empty_scavenged_tfchunk.FixtureData>
    {
        private TFChunk _chunk;
        
        public class FixtureData : SpecificationWithFilePerTestFixture
        {
            public readonly TFChunk _chunk;

            public FixtureData()
            {
                _chunk = TFChunk.CreateNew(Filename, 4096, 0, 0, isScavenged: true);
                _chunk.CompleteScavenge(new PosMap[0]);
                _chunk.CacheInMemory();
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
        }

        [Fact]
        public void no_record_at_exact_position_can_be_read()
        {
            Assert.False(_chunk.TryReadAt(0).Success);
        }

        [Fact]
        public void no_record_can_be_read_as_first_record()
        {
            Assert.False(_chunk.TryReadFirst().Success);
        }
        
        [Fact]
        public void no_record_can_be_read_as_closest_forward_record()
        {
            Assert.False(_chunk.TryReadClosestForward(0).Success);
        }

        [Fact]
        public void no_record_can_be_read_as_closest_backward_record()
        {
            Assert.False(_chunk.TryReadClosestBackward(0).Success);
        }

        [Fact]
        public void no_record_can_be_read_as_last_record()
        {
            Assert.False(_chunk.TryReadLast().Success);
        }
    }
}