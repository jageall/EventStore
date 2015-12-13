using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.HashCollisions
{
    public class with_three_collisioned_streams_one_event_each_read_index_should : ReadIndexTestScenario
    {
        private EventRecord _prepare1;
        private EventRecord _prepare2;
        private EventRecord _prepare3;

        protected override void WriteTestScenario()
        {
            var prepare1 = Fixture.WriteSingleEvent("AB", 0, "test1");
            var prepare2 = Fixture.WriteSingleEvent("CD", 0, "test2");
            var prepare3 = Fixture.WriteSingleEvent("EF", 0, "test3");
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._prepare1 = prepare1;
                instance._prepare2 = prepare2;
                instance._prepare3 = prepare3;
            });
        }

        [Fact]
        public void return_correct_last_event_version_for_first_stream()
        {
            Assert.Equal(0, ReadIndex.GetStreamLastEventNumber("AB"));
        }

        [Fact]
        public void return_correct_log_record_for_first_stream()
        {
            var result = ReadIndex.ReadEvent("AB", 0);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_prepare1, result.Record);
        }

        [Fact]
        public void not_find_record_with_index_1_in_first_stream()
        {
            var result = ReadIndex.ReadEvent("AB", 1);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);
        }

        [Fact]
        public void return_correct_range_on_from_start_range_query_for_first_stream()
        {
            var result = ReadIndex.ReadStreamEventsForward("AB", 0, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(_prepare1, result.Records[0]);
        }

        [Fact]
        public void return_correct_range_on_from_end_range_query_for_first_stream()
        {
            var result = ReadIndex.ReadStreamEventsBackward("AB", 0, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(_prepare1, result.Records[0]);
        }

        [Fact]
        public void return_correct_last_event_version_for_second_stream()
        {
            Assert.Equal(0, ReadIndex.GetStreamLastEventNumber("CD"));
        }

        [Fact]
        public void return_correct_log_record_for_second_stream()
        {
            var result = ReadIndex.ReadEvent("CD", 0);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_prepare2, result.Record);
        }

        [Fact]
        public void not_find_record_with_index_1_in_second_stream()
        {
            var result = ReadIndex.ReadEvent("CD", 1);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);
        }

        [Fact]
        public void return_correct_range_on_from_start_range_query_for_second_stream()
        {
            var result = ReadIndex.ReadStreamEventsForward("CD", 0, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(_prepare2, result.Records[0]);
        }

        [Fact]
        public void return_correct_range_on_from_end_range_query_for_second_stream()
        {
            var result = ReadIndex.ReadStreamEventsBackward("CD", 0, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(_prepare2, result.Records[0]);
        }

        [Fact]
        public void return_correct_last_event_version_for_third_stream()
        {
            Assert.Equal(0, ReadIndex.GetStreamLastEventNumber("EF"));
        }

        [Fact]
        public void return_correct_log_record_for_third_stream()
        {
            var result = ReadIndex.ReadEvent("EF", 0);
            Assert.Equal(ReadEventResult.Success, result.Result);
            Assert.Equal(_prepare3, result.Record);
        }

        [Fact]
        public void not_find_record_with_index_1_in_third_stream()
        {
            var result = ReadIndex.ReadEvent("EF", 1);
            Assert.Equal(ReadEventResult.NotFound, result.Result);
            Assert.Null(result.Record);
        }

        [Fact]
        public void return_correct_range_on_from_start_range_query_for_third_stream()
        {
            var result = ReadIndex.ReadStreamEventsForward("EF", 0, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(_prepare3, result.Records[0]);
        }

        [Fact]
        public void return_correct_range_on_from_end_range_query_for_third_stream()
        {
            var result = ReadIndex.ReadStreamEventsBackward("EF", 0, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(1, result.Records.Length);
            Assert.Equal(_prepare3, result.Records[0]);
        }

        [Fact]
        public void return_empty_range_when_asked_to_get_few_events_from_start_starting_from_1_in_first_stream()
        {
            var result = ReadIndex.ReadStreamEventsForward("AB", 1, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(0, result.Records.Length);
        }

        [Fact]
        public void return_empty_range_when_asked_to_get_few_events_from_start_starting_from_1_in_second_stream()
        {
            var result = ReadIndex.ReadStreamEventsForward("CD", 1, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(0, result.Records.Length);
        }

        [Fact]
        public void return_empty_range_when_asked_to_get_few_events_from_start_starting_from_1_in_third_stream()
        {
            var result = ReadIndex.ReadStreamEventsForward("EF", 1, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(0, result.Records.Length);
        }

        [Fact]
        public void return_empty_range_when_asked_to_get_few_events_from_end_starting_from_1_in_first_stream()
        {
            var result = ReadIndex.ReadStreamEventsBackward("AB", 1, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(0, result.Records.Length);
        }

        [Fact]
        public void return_empty_range_when_asked_to_get_few_events_from_end_starting_from_1_in_second_stream()
        {
            var result = ReadIndex.ReadStreamEventsBackward("CD", 1, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(0, result.Records.Length);
        }

        [Fact]
        public void return_empty_range_when_asked_to_get_few_events_from_end_starting_from_1_in_third_stream()
        {
            var result = ReadIndex.ReadStreamEventsBackward("EF", 1, 1);
            Assert.Equal(ReadStreamResult.Success, result.Result);
            Assert.Equal(0, result.Records.Length);
        }

        [Fact]
        public void return_correct_last_event_version_for_nonexistent_stream_with_same_hash()
        {
            Assert.Equal(-1, ReadIndex.GetStreamLastEventNumber("ZZ"));
        }

        [Fact]
        public void not_find_log_record_for_nonexistent_stream_with_same_hash()
        {
            var result = ReadIndex.ReadEvent("ZZ", 0);
            Assert.Equal(ReadEventResult.NoStream, result.Result);
            Assert.Null(result.Record);
        }

        [Fact]
        public void return_empty_range_on_from_start_range_query_for_non_existing_stream_with_same_hash()
        {
            var result = ReadIndex.ReadStreamEventsForward("ZZ", 0, 1);
            Assert.Equal(ReadStreamResult.NoStream, result.Result);
            Assert.Equal(0, result.Records.Length);
        }

        [Fact]
        public void return_empty_range_on_from_end_range_query_for_non_existing_stream_with_same_hash()
        {
            var result = ReadIndex.ReadStreamEventsBackward("ZZ", 0, 1);
            Assert.Equal(ReadStreamResult.NoStream, result.Result);
            Assert.Equal(0, result.Records.Length);
        }

        public with_three_collisioned_streams_one_event_each_read_index_should(FixtureData fixture) : base(fixture)
        {
        }
    }
}