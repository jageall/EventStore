using System;
using EventStore.Core.Data;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount.ReadRangeAndNextEventNumber
{
    public class when_reading_stream_with_max_age_and_max_count_and_max_age_is_more_strict: ReadIndexTestScenario
    {
        private EventRecord _event3;
        private EventRecord _event4;
        private EventRecord _event5;

        
        protected override void WriteTestScenario()
        {
            var now = DateTime.UtcNow;

            var metadata = string.Format(@"{{""$maxAge"":{0},""$maxCount"":5}}", (int)TimeSpan.FromMinutes(20).TotalSeconds);

                      Fixture.WriteStreamMetadata("ES", 0, metadata);
                      Fixture.WriteSingleEvent("ES", 0, "bla", now.AddMinutes(-100));
                      Fixture.WriteSingleEvent("ES", 1, "bla", now.AddMinutes(-50));
                      Fixture.WriteSingleEvent("ES", 2, "bla", now.AddMinutes(-25));
            var event3 = Fixture.WriteSingleEvent("ES", 3, "bla", now.AddMinutes(-15));
            var event4 = Fixture.WriteSingleEvent("ES", 4, "bla", now.AddMinutes(-11));
            var event5 = Fixture.WriteSingleEvent("ES", 5, "bla", now.AddMinutes(-3));

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._event3 = event3;
                instance._event4 = event4;
                instance._event5 = event5;
            });
        }

        [Fact]
        public void on_read_forward_from_start_to_expired_next_event_number_is_expired_by_age_plus_1_and_its_not_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsForward("ES", 0, 2);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(2, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.False(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(0, records.Length);
        }

        [Fact]
        public void on_read_forward_from_start_to_active_next_event_number_is_last_read_event_plus_1_and_its_not_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsForward("ES", 0, 5);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(5, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.False(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(2, records.Length);
            Assert.Equal(_event3, records[0]);
            Assert.Equal(_event4, records[1]);
        }

        [Fact]
        public void on_read_forward_from_expired_to_active_next_event_number_is_last_read_event_plus_1_and_its_not_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsForward("ES", 2, 2);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(4, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.False(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(1, records.Length);
            Assert.Equal(_event3, records[0]);
        }

        [Fact]
        public void on_read_forward_from_expired_to_end_next_event_number_is_end_plus_1_and_its_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsForward("ES", 2, 4);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(6, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.True(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(3, records.Length);
            Assert.Equal(_event3, records[0]);
            Assert.Equal(_event4, records[1]);
            Assert.Equal(_event5, records[2]);
        }

        [Fact]
        public void on_read_forward_from_expired_to_out_of_bounds_next_event_number_is_end_plus_1_and_its_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsForward("ES", 2, 6);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(6, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.True(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(3, records.Length);
            Assert.Equal(_event3, records[0]);
            Assert.Equal(_event4, records[1]);
            Assert.Equal(_event5, records[2]);
        }

        [Fact]
        public void on_read_forward_from_out_of_bounds_to_out_of_bounds_next_event_number_is_end_plus_1_and_its_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsForward("ES", 7, 2);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(6, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.True(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(0, records.Length);
        }


        [Fact]
        public void on_read_backward_from_end_to_active_next_event_number_is_last_read_event_minus_1_and_its_not_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsBackward("ES", 5, 2);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(3, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.False(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(2, records.Length);
            Assert.Equal(_event5, records[0]);
            Assert.Equal(_event4, records[1]);
        }

        [Fact]
        public void on_read_backward_from_end_to_maxage_bound_next_event_number_is_maxage_bound_minus_1_and_its_not_end_of_stream() // just no simple way to tell this
        {
            var res = ReadIndex.ReadStreamEventsBackward("ES", 5, 3);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(2, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.False(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(3, records.Length);
            Assert.Equal(_event5, records[0]);
            Assert.Equal(_event4, records[1]);
            Assert.Equal(_event3, records[2]);
        }

        [Fact]
        public void on_read_backward_from_active_to_expired_its_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsBackward("ES", 4, 3);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(-1, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.True(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(2, records.Length);
            Assert.Equal(_event4, records[0]);
            Assert.Equal(_event3, records[1]);
        }

        [Fact]
        public void on_read_backward_from_expired_to_expired_its_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsBackward("ES", 2, 2);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(-1, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.True(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(0, records.Length);
        }

        [Fact]
        public void on_read_backward_from_expired_to_before_start_its_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsBackward("ES", 2, 5);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(-1, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.True(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(0, records.Length);
        }

        [Fact]
        public void on_read_backward_from_out_of_bounds_to_out_of_bounds_next_event_number_is_end_and_its_not_end_of_stream()
        {
            var res = ReadIndex.ReadStreamEventsBackward("ES", 10, 3);
            Assert.Equal(ReadStreamResult.Success, res.Result);
            Assert.Equal(5, res.NextEventNumber);
            Assert.Equal(5, res.LastEventNumber);
            Assert.False(res.IsEndOfStream);

            var records = res.Records;
            Assert.Equal(0, records.Length);
        }

        public when_reading_stream_with_max_age_and_max_count_and_max_age_is_more_strict(FixtureData fixture) : base(fixture)
        {
        }
    }
}
