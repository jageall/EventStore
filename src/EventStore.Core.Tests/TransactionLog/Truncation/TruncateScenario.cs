using System;
using EventStore.Core.Tests.Services.Storage;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.Tests.TransactionLog.Truncation
{
    public abstract class TruncateScenario : ReadIndexTestScenario
    {
        protected TFChunkDbTruncator Truncator;
        protected long TruncateCheckpoint = long.MinValue;

        protected TruncateScenario(int maxEntriesInMemTable = 100, int metastreamMaxCount = 1)
            : base(maxEntriesInMemTable, metastreamMaxCount)
        {
        }

        protected override void AdditionalFixtureSetup()
        {
            
            if (TruncateCheckpoint == long.MinValue)
                throw new InvalidOperationException("AckCheckpoint must be set in WriteTestScenario.");
            var toStash = TruncateCheckpoint;
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance.TruncateCheckpoint = toStash;
            });
            OnBeforeTruncating();

            // need to close db before truncator can delete files

            Fixture.ReadIndex.Close();
            Fixture.ReadIndex.Dispose();

            Fixture.TableIndex.Close(removeFiles: false);

            Fixture.Db.Close();
            Fixture.Db.Dispose();

            var truncator = new TFChunkDbTruncator(Fixture.Db.Config);
            truncator.TruncateDb(TruncateCheckpoint);
        }

        protected virtual void OnBeforeTruncating()
        {
        }
    }
}