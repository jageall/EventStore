using System.Linq;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Scavenging
{
    public class when_stream_is_deleted : ScavengeTestScenario
    {
        public when_stream_is_deleted(Fixture fixture) : base(fixture)
        {
            
        }
        protected override DbResult CreateDb(TFChunkDbCreationHelper dbCreator)
        {
            return dbCreator
                    .Chunk(Rec.Prepare(0, "bla"), Rec.Prepare(0, "bla"), Rec.Commit(0, "bla"))
                    .Chunk(Rec.Delete(1, "bla"), Rec.Commit(1, "bla"))
                    .CompleteLastChunk()
                    .CreateDb();
        }

        protected override LogRecord[][] KeptRecords(DbResult dbResult)
        {
            return new []
            {
                new LogRecord[0], 
                dbResult.Recs[1]
            };
        }

        [Fact]
        public void stream_created_and_delete_tombstone_with_corresponding_commits_are_kept()
        {
            CheckRecords();
        }
    }
}