using Xunit;

namespace EventStore.Core.Tests.Services.Storage.DeletingStream
{
    public class when_deleting_stream_spanning_through_multiple_chunks_and_1_stream_with_same_hash_and_1_stream_with_different_hash_read_index_should 
        : ReadIndexTestScenario
    {
        protected override void WriteTestScenario()
        {
            Fixture.WriteSingleEvent("ES1", 0, new string('.', 3000));
            Fixture.WriteSingleEvent("ES1", 1, new string('.', 3000));
            Fixture.WriteSingleEvent("ES2", 0, new string('.', 3000));

            Fixture.WriteSingleEvent("ES", 0, new string('.', 3000), retryOnFail: true); // chunk 2
            Fixture.WriteSingleEvent("ES", 1, new string('.', 3000));
            Fixture.WriteSingleEvent("ES1", 2, new string('.', 3000));

            Fixture.WriteSingleEvent("ES2", 1, new string('.', 3000), retryOnFail: true); // chunk 3
            Fixture.WriteSingleEvent("ES1", 3, new string('.', 3000));
            Fixture.WriteSingleEvent("ES1", 4, new string('.', 3000));

            Fixture.WriteSingleEvent("ES2", 2, new string('.', 3000), retryOnFail: true); // chunk 4
            Fixture.WriteSingleEvent("ES", 2, new string('.', 3000));
            Fixture.WriteSingleEvent("ES", 3, new string('.', 3000));

            Fixture.WriteDelete("ES1");
        }

        [Fact]
        public void indicate_that_stream_is_deleted()
        {
            Assert.True(ReadIndex.IsStreamDeleted("ES1"));
        }

        [Fact]
        public void indicate_that_nonexisting_stream_with_same_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("XXX"));
        }

        [Fact]
        public void indicate_that_nonexisting_stream_with_different_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("XXXX"));
        }

        [Fact]
        public void indicate_that_existing_stream_with_same_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("ES2"));
        }

        [Fact]
        public void indicate_that_existing_stream_with_different_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("ES"));
        }

        public when_deleting_stream_spanning_through_multiple_chunks_and_1_stream_with_same_hash_and_1_stream_with_different_hash_read_index_should(FixtureData fixture) : base(fixture)
        {
        }
    }
}