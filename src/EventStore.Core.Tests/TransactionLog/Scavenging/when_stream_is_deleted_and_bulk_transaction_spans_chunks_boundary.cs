﻿using System.Linq;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Scavenging
{
    public class when_stream_is_deleted_and_bulk_transaction_spans_chunks_boundary : ScavengeTestScenario
    {
        public when_stream_is_deleted_and_bulk_transaction_spans_chunks_boundary(Fixture fixture) : base(fixture)
        {
            
        }
        protected override DbResult CreateDb(TFChunkDbCreationHelper dbCreator)
        {
            return dbCreator
                    .Chunk(Rec.Prepare(0, "bla"),
                           Rec.Commit(0, "bla"),
                           Rec.Prepare(1, "bla"),
                           Rec.Prepare(1, "bla"),
                           Rec.Prepare(1, "bla"),
                           Rec.Prepare(1, "bla"))
                    .Chunk(Rec.Prepare(1, "bla"),
                           Rec.Prepare(1, "bla"),
                           Rec.Prepare(1, "bla"),
                           Rec.Commit(1, "bla"),
                           Rec.Delete(2, "bla"),
                           Rec.Commit(2, "bla"))
                    .CompleteLastChunk()
                    .CreateDb();
        }

        protected override LogRecord[][] KeptRecords(DbResult dbResult)
        {
            return new[]
            {
                new[]{ dbResult.Recs[0][2] }, // first prepare in commit that is in different chunk
                dbResult.Recs[1].Where((x, i) => i >= 3).ToArray(),
            };
        }

        [Fact]
        public void first_prepare_of_transaction_is_preserved()
        {
            CheckRecords();
        }
    }
}