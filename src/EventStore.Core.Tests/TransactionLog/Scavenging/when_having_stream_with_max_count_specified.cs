﻿using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Scavenging
{
    public class when_having_stream_with_max_count_specified : ScavengeTestScenario
    {
        public when_having_stream_with_max_count_specified(Fixture fixture) : base(fixture)
        {
            
        }
        protected override DbResult CreateDb(TFChunkDbCreationHelper dbCreator)
        {
            return dbCreator
                    .Chunk(Rec.Prepare(0, "$$bla", metadata: new StreamMetadata(maxCount: 3)),
                           Rec.Commit(0, "$$bla"),
                           Rec.Prepare(1, "bla"),
                           Rec.Commit(1, "bla"),
                           Rec.Prepare(2, "bla"),
                           Rec.Prepare(2, "bla"),
                           Rec.Prepare(2, "bla"),
                           Rec.Prepare(2, "bla"),
                           Rec.Prepare(2, "bla"),
                           Rec.Commit(2, "bla"),
                           Rec.Prepare(3, "bla"),
                           Rec.Prepare(3, "bla"),
                           Rec.Prepare(3, "bla"),
                           Rec.Prepare(3, "bla"),
                           Rec.Commit(3, "bla"))
                    .CompleteLastChunk()
                    .CreateDb();
        }

        protected override LogRecord[][] KeptRecords(DbResult dbResult)
        {
            return new[]
            {
                dbResult.Recs[0].Where((x, i) => new [] {0, 1, 11, 12, 13, 14}.Contains(i)).ToArray()
            };
        }

        [Fact]
        public void expired_prepares_are_scavenged()
        {
            CheckRecords();
        }
    }
}