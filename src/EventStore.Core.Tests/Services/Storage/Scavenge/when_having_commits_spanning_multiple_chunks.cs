using System;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.Scavenge
{
    public class when_having_deleted_stream_spanning_two_chunks: ReadIndexTestScenario
    {
        private long[] _survivors;
        private long[] _scavenged;

        protected override void WriteTestScenario()
        {
            long tmp;

            var r2 = LogRecord.Prepare(Fixture.WriterCheckpoint.ReadNonFlushed(),
                                       Guid.NewGuid(),
                                       Guid.NewGuid(),
                                       Fixture.WriterCheckpoint.ReadNonFlushed(),
                                       0,
                                       "s1",
                                       -1,
                                       PrepareFlags.Data | PrepareFlags.TransactionBegin,
                                       "event-type",
                                       new byte[3],
                                       new byte[3]);
            Assert.True(Fixture.Writer.Write(r2, out tmp));

            var r4 = Fixture.WritePrepare("s2", -1);
            var r5 = Fixture.WriteCommit(r4.LogPosition, "s2", 0);
            var r6 = Fixture.WriteDelete("s2");

            Fixture.Writer.CompleteChunk();

            var r7 = LogRecord.Prepare(Fixture.WriterCheckpoint.ReadNonFlushed(),
                                       Guid.NewGuid(),
                                       Guid.NewGuid(),
                                       r2.LogPosition,
                                       1,
                                       "s1",
                                       -1,
                                       PrepareFlags.Data | PrepareFlags.TransactionEnd,
                                       "event-type",
                                       new byte[3],
                                       new byte[3]);
            Assert.True(Fixture.Writer.Write(r7, out tmp));

            var r9 = Fixture.WritePrepare("s3", -1);
            var r10 = Fixture.WriteCommit(r9.LogPosition, "s3", 0);
            var r11 = Fixture.WriteDelete("s3");

            var r12 = Fixture.WriteCommit(r2.LogPosition, "s1", 0);
            var r13 = Fixture.WriteDelete("s1");

            Fixture.Writer.CompleteChunk();

            _survivors = new[]
                         {
                                 r6.LogPosition,
                                 r11.LogPosition,
                                 r13.LogPosition
                         };
            _scavenged = new[]
                         {
                                 r2.LogPosition,
                                 r4.LogPosition,
                                 r5.LogPosition,
                                 r7.LogPosition,
                                 r9.LogPosition,
                                 r10.LogPosition,
                                 r12.LogPosition
                         };

            Fixture.Scavenge(completeLast: false, mergeChunks: true);
        }

        [Fact]
        public void stream_is_scavenged_after_merging_scavenge()
        {
            foreach (var logPos in _scavenged)
            {
                var chunk = Db.Manager.GetChunkFor(logPos);
                Assert.False(chunk.TryReadAt(logPos).Success);
            }

            foreach (var logPos in _survivors)
            {
                var chunk = Db.Manager.GetChunkFor(logPos);
                Assert.True(chunk.TryReadAt(logPos).Success);
            }
        }
    }
}
