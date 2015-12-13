using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.CheckCommitStartingAt
{
    public class when_writing_few_prepares_and_committing_one :ReadIndexTestScenario
    {
        private PrepareLogRecord _prepare0;
        private PrepareLogRecord _prepare1;
        private PrepareLogRecord _prepare2;

        protected override void WriteTestScenario()
        {
            var prepare0 = Fixture.WritePrepare("ES", expectedVersion: -1);
            var prepare1 = Fixture.WritePrepare("ES", expectedVersion: 0);
            var prepare2 = Fixture.WritePrepare("ES", expectedVersion: 1);
            Fixture.WriteCommit(prepare0.LogPosition, "ES", eventNumber: 0);
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance._prepare0 = prepare0;
                instance._prepare1 = prepare1;
                instance._prepare2 = prepare2;
            });
        }

        [Fact]
        public void check_commmit_on_2nd_prepare_should_return_ok_decision()
        {
            var res = ReadIndex.IndexWriter.CheckCommitStartingAt(_prepare1.LogPosition, WriterCheckpoint.ReadNonFlushed());

            Assert.Equal(CommitDecision.Ok, res.Decision);
            Assert.Equal("ES", res.EventStreamId);
            Assert.Equal(0, res.CurrentVersion);
            Assert.Equal(-1, res.StartEventNumber);
            Assert.Equal(-1, res.EndEventNumber);
        }

        [Fact]
        public void check_commmit_on_3rd_prepare_should_return_wrong_expected_version()
        {
            var res = ReadIndex.IndexWriter.CheckCommitStartingAt(_prepare2.LogPosition, WriterCheckpoint.ReadNonFlushed());

            Assert.Equal(CommitDecision.WrongExpectedVersion, res.Decision);
            Assert.Equal("ES", res.EventStreamId);
            Assert.Equal(0, res.CurrentVersion);
            Assert.Equal(-1, res.StartEventNumber);
            Assert.Equal(-1, res.EndEventNumber);
        }

        public when_writing_few_prepares_and_committing_one(FixtureData fixture) : base(fixture)
        {
        }
    }
}
