using Xunit;

namespace EventStore.Core.Tests.Services.Storage.DeletingStream
{
    public class when_deleting_stream_spanning_through_multiple_chunks_read_index_should : ReadIndexTestScenario
    {

        protected override void WriteTestScenario()
        {
            Fixture.WriteSingleEvent("ES", 0, new string('.', 3000));
            Fixture.WriteSingleEvent("ES", 1, new string('.', 3000));
            Fixture.WriteSingleEvent("ES", 2, new string('.', 3000));
            Fixture.WriteSingleEvent("ES", 3, new string('.', 3000), retryOnFail: true); // chunk 2
            Fixture.WriteSingleEvent("ES", 4, new string('.', 3000));
            Fixture.WriteSingleEvent("ES", 5, new string('.', 3000));
            Fixture.WriteSingleEvent("ES", 6, new string('.', 3000), retryOnFail: true); // chunk 3

            Fixture.WriteDelete("ES");
        }

        [Fact]
        public void indicate_that_stream_is_deleted()
        {
            Assert.True(ReadIndex.IsStreamDeleted("ES"));
        }

        [Fact]
        public void indicate_that_nonexisting_stream_with_same_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("ZZ"));
        }

        [Fact]
        public void indicate_that_nonexisting_stream_with_different_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("XXX"));
        }

        public when_deleting_stream_spanning_through_multiple_chunks_read_index_should(FixtureData fixture) : base(fixture)
        {
        }
    }
}