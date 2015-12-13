using System.IO;
using EventStore.Core.DataStructures;
using EventStore.Core.Index;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Services.Storage;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;

namespace EventStore.Core.Tests.TransactionLog.Truncation
{
    public abstract class TruncateAndReOpenDbScenario : TruncateScenario
    {
        protected TruncateAndReOpenDbScenario(FixtureData fixture) : this(fixture, 100, 1) { }
        protected TruncateAndReOpenDbScenario(FixtureData fixture, int maxEntriesInMemTable, int metastreamMaxCount)
            : base(fixture,maxEntriesInMemTable, metastreamMaxCount)
        {
        }

        protected override void AdditionalFixtureSetup()
        {
            base.AdditionalFixtureSetup();

            ReOpenDb();
        }

        private void ReOpenDb()
        {
            Fixture.Db = new TFChunkDb(new TFChunkDbConfig(Fixture.PathName,
                                                   new VersionedPatternFileNamingStrategy(Fixture.PathName, "chunk-"),
                                                   10000,
                                                   0,
                                                   Fixture.WriterCheckpoint,
                                                   Fixture.ChaserCheckpoint,
                                                   new InMemoryCheckpoint(-1),
                                                   new InMemoryCheckpoint(-1)));

            Fixture.Db.Open();

            var readers = new ObjectPool<ITransactionFileReader>("Readers", 2, 5, () => new TFChunkReader(Fixture.Db, Fixture.Db.Config.WriterCheckpoint));
            Fixture.TableIndex = new TableIndex(Path.Combine(Fixture.PathName, "index"),
                                        () => new HashListMemTable(MaxEntriesInMemTable * 2),
                                        () => new TFReaderLease(readers),
                                        MaxEntriesInMemTable);
            Fixture.ReadIndex = new ReadIndex(new NoopPublisher(),
                                      readers,
                                      Fixture.TableIndex,
                                      new ByLengthHasher(),
                                      0,
                                      additionalCommitChecks: true,
                                      metastreamMaxCount: MetastreamMaxCount);
            Fixture.ReadIndex.Init(Fixture.ChaserCheckpoint.Read());
        }
    }
}