using System;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.Transactions
{
    public class when_having_two_intermingled_transactions_and_some_uncommited_prepares_spanning_few_chunks_read_index_should : ReadIndexTestScenario
    {
        private EventRecord _p1;
        private EventRecord _p2;
        private EventRecord _p3;
        private EventRecord _p4;
        private EventRecord _p5;

        private long _pos6;
        private long _pos7;
        private long _pos8;

        private long _t1CommitPos;
        private long _t2CommitPos;

        protected override void WriteTestScenario()
        {
            var t1 = Fixture.WriteTransactionBegin("ES", ExpectedVersion.NoStream);
            var t2 = Fixture.WriteTransactionBegin("ABC", ExpectedVersion.NoStream);

            var p1 = Fixture.WriteTransactionEvent(t1.CorrelationId, t1.LogPosition, 0, t1.EventStreamId, 0, "es1" + new string('.', 3000), PrepareFlags.Data);
            var p2 = Fixture.WriteTransactionEvent(t2.CorrelationId, t2.LogPosition, 0, t2.EventStreamId, 0, "abc1" + new string('.', 3000), PrepareFlags.Data);
            var p3 = Fixture.WriteTransactionEvent(t1.CorrelationId, t1.LogPosition, 1, t1.EventStreamId, 1, "es1" + new string('.', 3000), PrepareFlags.Data);
            var p4 = Fixture.WriteTransactionEvent(t2.CorrelationId, t2.LogPosition, 1, t2.EventStreamId, 1, "abc1" + new string('.', 3000), PrepareFlags.Data, retryOnFail: true);
            var p5 = Fixture.WriteTransactionEvent(t1.CorrelationId, t1.LogPosition, 2, t1.EventStreamId, 2, "es1" + new string('.', 3000), PrepareFlags.Data);

            Fixture.WriteTransactionEnd(t2.CorrelationId, t2.TransactionPosition, t2.EventStreamId);
            Fixture.WriteTransactionEnd(t1.CorrelationId, t1.TransactionPosition, t1.EventStreamId);

            var t2CommitPos = Fixture.WriteCommit(t2.CorrelationId, t2.TransactionPosition, t2.EventStreamId, p2.EventNumber);
            var t1CommitPos = Fixture.WriteCommit(t1.CorrelationId, t1.TransactionPosition, t1.EventStreamId, p1.EventNumber);

            var pos6 = Fixture.Db.Config.WriterCheckpoint.ReadNonFlushed();
            var r6 = LogRecord.Prepare(pos6, Guid.NewGuid(), Guid.NewGuid(), pos6, 0, "t1", -1, PrepareFlags.SingleWrite, "et", LogRecord.NoData, LogRecord.NoData);
            long pos7;
            Fixture.Writer.Write(r6, out pos7);
            var r7 = LogRecord.Prepare(pos7, Guid.NewGuid(), Guid.NewGuid(), pos7, 0, "t1", -1, PrepareFlags.SingleWrite, "et", LogRecord.NoData, LogRecord.NoData);
            long pos8;
            Fixture.Writer.Write(r7, out pos8);
            var r8 = LogRecord.Prepare(pos8, Guid.NewGuid(), Guid.NewGuid(), pos8, 0, "t1", -1, PrepareFlags.SingleWrite, "et", LogRecord.NoData, LogRecord.NoData);
            long pos9;
            Fixture.Writer.Write(r8, out pos9);

            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._p1 = p1;
                instance._p2 = p2;
                instance._p3 = p3;
                instance._p4 = p4;
                instance._p5 = p5;

                instance._pos6 = pos6;
                instance._pos7 = pos7;
                instance._pos8 = pos8;
                instance._t1CommitPos = t1CommitPos;
                instance._t2CommitPos = t2CommitPos;
            });
        }

        [Fact]
        public void read_all_events_forward_returns_all_events_in_correct_order()
        {
            var records = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 10).Records;

            Assert.Equal(5, records.Count);
            Assert.Equal(_p2, records[0].Event);
            Assert.Equal(_p4, records[1].Event);
            Assert.Equal(_p1, records[2].Event);
            Assert.Equal(_p3, records[3].Event);
            Assert.Equal(_p5, records[4].Event);
        }

        [Fact]
        public void read_all_events_backward_returns_all_events_in_correct_order()
        {
            var pos = GetBackwardReadPos();
            var records = ReadIndex.ReadAllEventsBackward(pos, 10).Records;

            Assert.Equal(5, records.Count);
            Assert.Equal(_p5, records[0].Event);
            Assert.Equal(_p3, records[1].Event);
            Assert.Equal(_p1, records[2].Event);
            Assert.Equal(_p4, records[3].Event);
            Assert.Equal(_p2, records[4].Event);
        }

        [Fact]
        public void read_all_events_forward_returns_nothing_when_prepare_position_is_greater_than_last_prepare_in_commit()
        {
            var records = ReadIndex.ReadAllEventsForward(new TFPos(_t1CommitPos, _t1CommitPos), 10).Records;
            Assert.Equal(0, records.Count);
        }

        [Fact]
        public void read_all_events_backwards_returns_nothing_when_prepare_position_is_smaller_than_first_prepare_in_commit()
        {
            var records = ReadIndex.ReadAllEventsBackward(new TFPos(_t2CommitPos, 0), 10).Records;
            Assert.Equal(0, records.Count);
        }

        [Fact]
        public void read_all_events_forward_returns_correct_events_starting_in_the_middle_of_tf()
        {
            var res1 = ReadIndex.ReadAllEventsForward(new TFPos(_t2CommitPos, _p4.LogPosition), 10);

            Assert.Equal(4, res1.Records.Count);
            Assert.Equal(_p4, res1.Records[0].Event);
            Assert.Equal(_p1, res1.Records[1].Event);
            Assert.Equal(_p3, res1.Records[2].Event);
            Assert.Equal(_p5, res1.Records[3].Event);

            var res2 = ReadIndex.ReadAllEventsBackward(res1.PrevPos, 10);
            Assert.Equal(1, res2.Records.Count);
            Assert.Equal(_p2, res2.Records[0].Event);
        }

        [Fact]
        public void read_all_events_backward_returns_correct_events_starting_in_the_middle_of_tf()
        {
            var pos = new TFPos(_pos6, _p4.LogPosition); // p3 post-pos
            var res1 = ReadIndex.ReadAllEventsBackward(pos, 10);

            Assert.Equal(4, res1.Records.Count);
            Assert.Equal(_p3, res1.Records[0].Event);
            Assert.Equal(_p1, res1.Records[1].Event);
            Assert.Equal(_p4, res1.Records[2].Event);
            Assert.Equal(_p2, res1.Records[3].Event);

            var res2 = ReadIndex.ReadAllEventsForward(res1.PrevPos, 10);
            Assert.Equal(1, res2.Records.Count);
            Assert.Equal(_p5, res2.Records[0].Event);
        }

        [Fact]
        public void all_records_can_be_read_sequentially_page_by_page_in_forward_pass()
        {
            var recs = new[] {_p2, _p4, _p1, _p3, _p5}; // in committed order

            int count = 0;
            var pos = new TFPos(0, 0);
            IndexReadAllResult result;
            while ((result = ReadIndex.ReadAllEventsForward(pos, 1)).Records.Count != 0)
            {
                Assert.Equal(1, result.Records.Count);
                Assert.Equal(recs[count], result.Records[0].Event);
                pos = result.NextPos;
                count += 1;
            }
            Assert.Equal(recs.Length, count);
        }

        [Fact]
        public void all_records_can_be_read_sequentially_page_by_page_in_backward_pass()
        {
            var recs = new[] { _p5, _p3, _p1, _p4, _p2 }; // in reverse committed order

            int count = 0;
            var pos = GetBackwardReadPos();
            IndexReadAllResult result;
            while ((result = ReadIndex.ReadAllEventsBackward(pos, 1)).Records.Count != 0)
            {
                Assert.Equal(1, result.Records.Count);
                Assert.Equal(recs[count], result.Records[0].Event);
                pos = result.NextPos;
                count += 1;
            }
            Assert.Equal(recs.Length, count);
        }

        [Fact]
        public void position_returned_for_prev_page_when_traversing_forward_allow_to_traverse_backward_correctly()
        {
            var recs = new[] { _p2, _p4, _p1, _p3, _p5 }; // in committed order

            int count = 0;
            var pos = new TFPos(0, 0);
            IndexReadAllResult result;
            while ((result = ReadIndex.ReadAllEventsForward(pos, 1)).Records.Count != 0)
            {
                Assert.Equal(1, result.Records.Count);
                Assert.Equal(recs[count], result.Records[0].Event);

                var localPos = result.PrevPos;
                int localCount = 0;
                IndexReadAllResult localResult;
                while ((localResult = ReadIndex.ReadAllEventsBackward(localPos, 1)).Records.Count != 0)
                {
                    Assert.Equal(1, localResult.Records.Count);
                    Assert.Equal(recs[count - 1 - localCount], localResult.Records[0].Event);
                    localPos = localResult.NextPos;
                    localCount += 1;
                }

                pos = result.NextPos;
                count += 1;
            }
            Assert.Equal(recs.Length, count);
        }

        [Fact]
        public void position_returned_for_prev_page_when_traversing_backward_allow_to_traverse_forward_correctly()
        {
            var recs = new[] { _p5, _p3, _p1, _p4, _p2 }; // in reverse committed order

            int count = 0;
            var pos = GetBackwardReadPos();
            IndexReadAllResult result;
            while ((result = ReadIndex.ReadAllEventsBackward(pos, 1)).Records.Count != 0)
            {
                Assert.Equal(1, result.Records.Count);
                Assert.Equal(recs[count], result.Records[0].Event);

                var localPos = result.PrevPos;
                int localCount = 0;
                IndexReadAllResult localResult;
                while ((localResult = ReadIndex.ReadAllEventsForward(localPos, 1)).Records.Count != 0)
                {
                    Assert.Equal(1, localResult.Records.Count);
                    Assert.Equal(recs[count - 1 - localCount], localResult.Records[0].Event);
                    localPos = localResult.NextPos;
                    localCount += 1;
                }

                pos = result.NextPos;
                count += 1;
            }
            Assert.Equal(recs.Length, count);
        }

        [Fact]
        public void reading_all_forward_at_position_with_no_commits_after_returns_prev_pos_that_allows_to_traverse_back()
        {
            var res1 = ReadIndex.ReadAllEventsForward(new TFPos(_pos6, 0), 100);
            Assert.Equal(0, res1.Records.Count);

            var recs = new[] { _p5, _p3, _p1, _p4, _p2 }; // in reverse committed order
            int count = 0;
            IndexReadAllResult result;
            TFPos pos = res1.PrevPos;
            while ((result = ReadIndex.ReadAllEventsBackward(pos, 1)).Records.Count != 0)
            {
                Assert.Equal(1, result.Records.Count);
                Assert.Equal(recs[count], result.Records[0].Event);
                pos = result.NextPos;
                count += 1;
            }
            Assert.Equal(recs.Length, count);
        }

        [Fact]
        public void reading_all_forward_at_the_very_end_returns_prev_pos_that_allows_to_traverse_back()
        {
            var res1 = ReadIndex.ReadAllEventsForward(new TFPos(Db.Config.WriterCheckpoint.Read(), 0), 100);
            Assert.Equal(0, res1.Records.Count);

            var recs = new[] { _p5, _p3, _p1, _p4, _p2 }; // in reverse committed order
            int count = 0;
            IndexReadAllResult result;
            TFPos pos = res1.PrevPos;
            while ((result = ReadIndex.ReadAllEventsBackward(pos, 1)).Records.Count != 0)
            {
                Assert.Equal(1, result.Records.Count);
                Assert.Equal(recs[count], result.Records[0].Event);
                pos = result.NextPos;
                count += 1;
            }
            Assert.Equal(recs.Length, count);
        }

        [Fact]
        public void reading_all_backward_at_position_with_no_commits_before_returns_prev_pos_that_allows_to_traverse_forward()
        {
            var res1 = ReadIndex.ReadAllEventsBackward(new TFPos(_t2CommitPos, int.MaxValue), 100);
            Assert.Equal(0, res1.Records.Count);

            var recs = new[] { _p2, _p4, _p1, _p3, _p5 };
            int count = 0;
            IndexReadAllResult result;
            TFPos pos = res1.PrevPos;
            while ((result = ReadIndex.ReadAllEventsForward(pos, 1)).Records.Count != 0)
            {
                Assert.Equal(1, result.Records.Count);
                Assert.Equal(recs[count], result.Records[0].Event);
                pos = result.NextPos;
                count += 1;
            }
            Assert.Equal(recs.Length, count);
        }

        [Fact]
        public void reading_all_backward_at_the_very_beginning_returns_prev_pos_that_allows_to_traverse_forward()
        {
            var res1 = ReadIndex.ReadAllEventsBackward(new TFPos(0, int.MaxValue), 100);
            Assert.Equal(0, res1.Records.Count);

            var recs = new[] { _p2, _p4, _p1, _p3, _p5 };
            int count = 0;
            IndexReadAllResult result;
            TFPos pos = res1.PrevPos;
            while ((result = ReadIndex.ReadAllEventsForward(pos, 1)).Records.Count != 0)
            {
                Assert.Equal(1, result.Records.Count);
                Assert.Equal(recs[count], result.Records[0].Event);
                pos = result.NextPos;
                count += 1;
            }
            Assert.Equal(recs.Length, count);
        }

        public when_having_two_intermingled_transactions_and_some_uncommited_prepares_spanning_few_chunks_read_index_should(FixtureData fixture) : base(fixture)
        {
        }
    }
}