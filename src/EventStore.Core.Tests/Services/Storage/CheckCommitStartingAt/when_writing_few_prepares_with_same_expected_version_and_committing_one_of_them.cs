﻿using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.CheckCommitStartingAt
{
    public class when_writing_few_prepares_with_same_expected_version_and_committing_one_of_them :ReadIndexTestScenario
    {
        private PrepareLogRecord _prepare0;
        private PrepareLogRecord _prepare1;
        private PrepareLogRecord _prepare2;

        protected override void WriteTestScenario()
        {
            _prepare0 = Fixture.WritePrepare("ES", expectedVersion: -1);
            _prepare1 = Fixture.WritePrepare("ES", expectedVersion: -1);
            _prepare2 = Fixture.WritePrepare("ES", expectedVersion: -1);
            Fixture.WriteCommit(_prepare1.LogPosition, "ES", eventNumber: 0);
        }

        [Fact]
        public void other_prepares_cannot_be_committed()
        {
            var res = ReadIndex.IndexWriter.CheckCommitStartingAt(_prepare0.LogPosition, WriterCheckpoint.ReadNonFlushed());

            Assert.Equal(CommitDecision.WrongExpectedVersion, res.Decision);
            Assert.Equal("ES", res.EventStreamId);
            Assert.Equal(0, res.CurrentVersion);
            Assert.Equal(-1, res.StartEventNumber);
            Assert.Equal(-1, res.EndEventNumber);

            res = ReadIndex.IndexWriter.CheckCommitStartingAt(_prepare2.LogPosition, WriterCheckpoint.ReadNonFlushed());

            Assert.Equal(CommitDecision.WrongExpectedVersion, res.Decision);
            Assert.Equal("ES", res.EventStreamId);
            Assert.Equal(0, res.CurrentVersion);
            Assert.Equal(-1, res.StartEventNumber);
            Assert.Equal(-1, res.EndEventNumber);
        }
    }
}
