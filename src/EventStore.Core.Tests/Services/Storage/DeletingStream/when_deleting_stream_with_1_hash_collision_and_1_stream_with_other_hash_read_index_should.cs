using Xunit;

namespace EventStore.Core.Tests.Services.Storage.DeletingStream
{
    public class when_deleting_stream_with_1_hash_collision_and_1_stream_with_other_hash_read_index_should : ReadIndexTestScenario
    {
        protected override void WriteTestScenario()
        {
            Fixture.WriteSingleEvent("S1", 0, "bla1");
            Fixture.WriteSingleEvent("S1", 1, "bla1");
            Fixture.WriteSingleEvent("S2", 0, "bla1");
            Fixture.WriteSingleEvent("S2", 1, "bla1");
            Fixture.WriteSingleEvent("S1", 2, "bla1");
            Fixture.WriteSingleEvent("SSS", 0, "bla1");
            
            Fixture.WriteDelete("S1");
        }

        [Fact]
        public void indicate_that_stream_is_deleted()
        {
            Assert.True(ReadIndex.IsStreamDeleted("S1"));
        }

        [Fact]
        public void indicate_that_other_stream_with_same_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("S2"));
        }

        [Fact]
        public void indicate_that_other_stream_with_different_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("SSS"));
        }

        [Fact]
        public void indicate_that_not_existing_stream_with_same_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("XX"));
        }

        [Fact]
        public void indicate_that_not_existing_stream_with_different_hash_is_not_deleted()
        {
            Assert.False(ReadIndex.IsStreamDeleted("XXX"));
        }

        public when_deleting_stream_with_1_hash_collision_and_1_stream_with_other_hash_read_index_should(FixtureData fixture) : base(fixture)
        {
        }
    }
}